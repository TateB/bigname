use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContractSource, load_watched_contracts};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, load_name_surface_including_noncanonical, load_resource_including_noncanonical,
    load_surface_binding_including_noncanonical, load_token_lineage_including_noncanonical,
    upsert_name_surfaces, upsert_normalized_events, upsert_resources, upsert_surface_bindings,
    upsert_token_lineages,
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
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";

const DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const EVENT_KIND_AUTHORITY_EPOCH_CHANGED: &str = "AuthorityEpochChanged";
const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
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
const NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256,uint256)";
const ADDR_CHANGED_SIGNATURE: &str = "AddrChanged(bytes32,address)";
const ADDRESS_CHANGED_SIGNATURE: &str = "AddressChanged(bytes32,uint256,bytes)";
const NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
const TEXT_CHANGED_SIGNATURE: &str = "TextChanged(bytes32,string,string)";
const TRANSFER_SIGNATURE: &str = "Transfer(address,address,uint256)";
const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";

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
    pub by_kind: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
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
enum AuthorityObservation {
    RegistrationGranted(NameRegistrationObservation),
    RegistrationRenewed(NameRenewalObservation),
    TokenTransferred(TokenTransferObservation),
    RegistryOwnerChanged(RegistryOwnerObservation),
    ResolverChanged(ResolverObservation),
    RecordChanged(RecordChangeObservation),
    RecordVersionChanged(RecordVersionObservation),
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
}

impl AuthorityKind {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::RegistryOnly => "registry_only",
            Self::Registrar => "registrar",
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

impl AuthorityProfile {
    const fn namespace(self) -> &'static str {
        match self {
            Self::Ens => "ens",
            Self::Basenames => "basenames",
        }
    }

    const fn registrar_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
        }
    }

    const fn registry_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
        }
    }

    const fn resolver_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
        }
    }

    fn root_node(self) -> String {
        match self {
            Self::Ens => eth_node(),
            Self::Basenames => base_eth_node(),
        }
    }

    fn observe_name(self, label: &str, normalizer_version: &str) -> Result<NameMetadata> {
        observe_registrar_name_with_version(label, self, normalizer_version)
    }
}

fn default_registrar_source_family(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
        _ => SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    }
}

pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let canonical_blocks = load_canonical_blocks(pool, chain).await?;
    if canonical_blocks.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let block_index = CanonicalBlockIndex {
        blocks: canonical_blocks,
    };
    let reverse_claim_sources = load_reverse_claim_sources(pool, chain).await?;
    let raw_logs = load_authority_raw_logs(pool, chain, &active_emitters).await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut matched_log_count = 0usize;
    for raw_log in &raw_logs {
        let Some(observation) = build_authority_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;

        let labelhash = if let Some(namehash) = observation_namehash(&observation) {
            if let Some(labelhash) = namehash_to_labelhash.get(namehash).cloned() {
                labelhash
            } else if let Some(claim_source) = reverse_claim_sources.get(namehash).cloned() {
                let history = reverse_histories
                    .entry(namehash.to_owned())
                    .or_insert_with(|| ReverseClaimSourceHistory {
                        claim_source,
                        current_resolver: None,
                        current_record_version: None,
                        events: Vec::new(),
                    });
                apply_reverse_claim_source_observation(history, observation)?;
                continue;
            } else {
                continue;
            }
        } else {
            observation_labelhash(&observation)
        };
        let history = histories
            .entry(labelhash.clone())
            .or_insert_with(|| NameHistory {
                name: None,
                labelhash: labelhash.clone(),
                first_name_ref: None,
                current_registration: None,
                current_registry_owner: None,
                current_resolver: None,
                current_record_version: None,
                open_binding: None,
                bindings: Vec::new(),
                events: Vec::new(),
                registry_resource_anchor: None,
                latest_registry_owner_ref: None,
                latest_registry_owner_before_registration: None,
            });

        apply_observation(history, observation, &block_index).await?;
        if let Some(name) = history.name.as_ref() {
            namehash_to_labelhash.insert(name.namehash.clone(), labelhash);
        }
    }

    let head_block = block_index
        .blocks
        .last()
        .cloned()
        .context("canonical block index must contain a head block")?;
    let head_ref = BoundaryRef {
        chain_id: head_block.chain_id.clone(),
        block_hash: head_block.block_hash.clone(),
        block_number: head_block.block_number,
        block_timestamp: head_block.block_timestamp,
        canonicality_state: head_block.canonicality_state,
        namespace: active_emitters
            .first()
            .map(|emitter| emitter.namespace.clone())
            .unwrap_or_else(|| "ens".to_owned()),
    };

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = Vec::<NormalizedEvent>::new();

    for history in histories.into_values() {
        let Some(name) = history.name.clone() else {
            continue;
        };

        let finalized = finalize_history(history, &head_ref)?;
        if let Some(surface) =
            build_name_surface(pool, &name, finalized.first_name_ref.as_ref()).await?
        {
            surfaces.push(surface);
        }

        if let Some(registry_anchor) = finalized.registry_resource_anchor.as_ref() {
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!(
                        "resource:registry-only:{}:{}",
                        chain, finalized.labelhash
                    )),
                    None,
                    &registry_anchor.chain_id,
                    registry_anchor,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registry_only",
                        "authority_key": format!("registry-only:{}:{}", chain, finalized.labelhash),
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "current_registry_owner": finalized.current_registry_owner,
                    }),
                )
                .await?,
            );
        }

        for lease in &finalized.registrar_leases {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", lease.authority_key));
            token_lineages.push(
                build_token_lineage(
                    pool,
                    token_lineage_id,
                    &lease.start_ref.chain_id,
                    &lease.start_ref,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                    }),
                )
                .await?,
            );
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!("resource:{}", lease.authority_key)),
                    Some(token_lineage_id),
                    &lease.start_ref.chain_id,
                    &lease.start_ref.as_boundary_ref(),
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "expiry": lease.expiry.unix_timestamp(),
                        "registrant": lease.registrant,
                        "released_at": lease.release_ref.as_ref().map(|value| value.block_timestamp.unix_timestamp()),
                    }),
                )
                .await?,
            );
        }

        for segment in finalized.bindings {
            bindings.push(
                build_surface_binding(pool, &name.logical_name_id, &segment, &head_ref.chain_id)
                    .await?,
            );
        }
        events.extend(finalized.events);
    }
    for history in reverse_histories.into_values() {
        events.extend(history.events);
    }

    let by_kind = count_events_by_kind(&events);
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings(pool, &bindings).await?;
    upsert_normalized_events(pool, &events).await?;

    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        by_kind,
    })
}

#[derive(Clone, Debug)]
struct FinalizedHistory {
    labelhash: String,
    first_name_ref: Option<ObservationRef>,
    bindings: Vec<BindingSegment>,
    events: Vec<NormalizedEvent>,
    registrar_leases: Vec<RegistrationLease>,
    registry_resource_anchor: Option<BoundaryRef>,
    current_registry_owner: Option<String>,
}

fn finalize_history(mut history: NameHistory, head_ref: &BoundaryRef) -> Result<FinalizedHistory> {
    if let Some(lease) = history.current_registration.take() {
        if let Some(release_ref) = lease.release_ref.clone() {
            if release_ref.block_timestamp <= head_ref.block_timestamp {
                emit_registration_released_event(&mut history, &lease, &release_ref)?;
                let registry_after = registry_anchor_for_history(
                    &history,
                    &lease.reference_chain(),
                    &lease.labelhash,
                );
                transition_authority(
                    &mut history,
                    Some(build_registrar_anchor(&lease)),
                    registry_after.clone(),
                    &release_ref,
                    release_ref.block_timestamp,
                )?;
                if let (Some(name), Some(anchor), Some(subject)) = (
                    history.name.as_ref(),
                    registry_after.as_ref(),
                    nonzero_address(history.current_registry_owner.as_deref()),
                ) {
                    emit_boundary_permission_grants(
                        &mut history.events,
                        &release_ref,
                        &name.logical_name_id,
                        anchor,
                        &subject,
                        history.current_resolver.as_deref(),
                        &release_ref.chain_id,
                        EVENT_KIND_REGISTRATION_RELEASED,
                    );
                }
            } else if history.open_binding.is_none() {
                let registrar_anchor = build_registrar_anchor(&lease);
                history.open_binding = Some(OpenBinding {
                    surface_binding_id: deterministic_uuid(&format!(
                        "binding:{}:{}",
                        registrar_anchor.authority_key,
                        lease.start_ref.block_timestamp.unix_timestamp()
                    )),
                    authority: registrar_anchor,
                    active_from: lease.start_ref.block_timestamp,
                    anchor_ref: lease.start_ref.as_boundary_ref(),
                });
            }
        } else if history.open_binding.is_none() {
            let registrar_anchor = build_registrar_anchor(&lease);
            history.open_binding = Some(OpenBinding {
                surface_binding_id: deterministic_uuid(&format!(
                    "binding:{}:{}",
                    registrar_anchor.authority_key,
                    lease.start_ref.block_timestamp.unix_timestamp()
                )),
                authority: registrar_anchor,
                active_from: lease.start_ref.block_timestamp,
                anchor_ref: lease.start_ref.as_boundary_ref(),
            });
        }

        history.current_registration = Some(lease);
    }

    if history.open_binding.is_none()
        && history.current_registration.is_none()
        && history
            .current_registry_owner
            .as_deref()
            .is_some_and(|owner| owner != ZERO_ADDRESS)
        && let Some(anchor) =
            registry_anchor_for_history(&history, &head_ref.chain_id, &history.labelhash)
    {
        history.open_binding = Some(OpenBinding {
            surface_binding_id: deterministic_uuid(&format!(
                "binding:{}:{}",
                anchor.authority_key,
                anchor
                    .binding_manifest_id
                    .checked_mul(0)
                    .unwrap_or_default()
                    + head_ref.block_timestamp.unix_timestamp()
            )),
            authority: anchor,
            active_from: head_ref.block_timestamp,
            anchor_ref: head_ref.clone(),
        });
    }

    if let Some(open_binding) = history.open_binding.take() {
        history.bindings.push(BindingSegment {
            surface_binding_id: open_binding.surface_binding_id,
            authority: open_binding.authority,
            active_from: open_binding.active_from,
            active_to: None,
            anchor_ref: open_binding.anchor_ref,
        });
    }

    let registrar_leases = history.current_registration.into_iter().collect::<Vec<_>>();

    Ok(FinalizedHistory {
        labelhash: history.labelhash,
        first_name_ref: history.first_name_ref,
        bindings: history.bindings,
        events: history.events,
        registrar_leases,
        registry_resource_anchor: history.registry_resource_anchor,
        current_registry_owner: history.current_registry_owner,
    })
}

fn build_registrar_anchor(lease: &RegistrationLease) -> AuthorityAnchor {
    AuthorityAnchor {
        kind: AuthorityKind::Registrar,
        authority_key: lease.authority_key.clone(),
        resource_id: deterministic_uuid(&format!("resource:{}", lease.authority_key)),
        token_lineage_id: Some(deterministic_uuid(&format!(
            "token-lineage:{}",
            lease.authority_key
        ))),
        binding_source_family: lease.start_ref.source_family.clone(),
        binding_manifest_version: lease.start_ref.manifest_version,
        binding_manifest_id: lease.start_ref.source_manifest_id,
    }
}

fn registry_anchor_for_history(
    history: &NameHistory,
    chain: &str,
    labelhash: &str,
) -> Option<AuthorityAnchor> {
    if history
        .current_registry_owner
        .as_deref()
        .is_none_or(|owner| owner == ZERO_ADDRESS)
    {
        return None;
    }

    let reference = history
        .latest_registry_owner_ref
        .as_ref()
        .or(history.latest_registry_owner_before_registration.as_ref())?;
    Some(AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key: format!("registry-only:{chain}:{labelhash}"),
        resource_id: deterministic_uuid(&format!("resource:registry-only:{chain}:{labelhash}")),
        token_lineage_id: None,
        binding_source_family: reference.source_family.clone(),
        binding_manifest_version: reference.manifest_version,
        binding_manifest_id: reference.source_manifest_id,
    })
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_default() += 1;
    }
    counts
}

async fn load_reverse_claim_sources(
    pool: &PgPool,
    chain: &str,
) -> Result<HashMap<String, ReverseClaimSource>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (LOWER(ne.after_state->>'reverse_node'))
            LOWER(ne.after_state->>'reverse_node') AS reverse_node,
            LOWER(ne.after_state->>'address') AS address,
            COALESCE(ne.after_state->>'namespace', ne.namespace) AS namespace,
            ne.after_state->>'coin_type' AS coin_type,
            ne.after_state->>'reverse_name' AS reverse_name,
            COALESCE(
                ne.after_state->'claim_provenance'->>'source_family',
                ne.source_family
            ) AS claim_source_family,
            COALESCE(
                ne.after_state->'claim_provenance'->>'contract_role',
                $3
            ) AS claim_contract_role,
            ne.after_state->'claim_provenance'->>'contract_instance_id' AS claim_contract_instance_id,
            COALESCE(
                ne.after_state->'claim_provenance'->>'emitting_address',
                ne.raw_fact_ref->>'emitting_address'
            ) AS claim_emitting_address
        FROM normalized_events ne
        WHERE ne.chain_id = $1
          AND COALESCE(ne.after_state->>'namespace', ne.namespace) IN ($2, $3)
          AND ne.event_kind = $5
          AND ne.derivation_kind = $6
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND ne.after_state->>'reverse_node' IS NOT NULL
          AND ne.after_state->>'reverse_node' <> ''
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
          AND ne.after_state->>'reverse_name' IS NOT NULL
          AND ne.after_state->>'reverse_name' <> ''
        ORDER BY
            LOWER(ne.after_state->>'reverse_node'),
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        "#,
    )
    .bind(chain)
    .bind("ens")
    .bind("basenames")
    .bind(CONTRACT_ROLE_REVERSE_REGISTRAR)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(DERIVATION_KIND_ENS_V1_REVERSE_CLAIM)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load reverse claim sources for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let reverse_node = row
                .try_get::<String, _>("reverse_node")
                .context("missing reverse_node")?;
            let address = row
                .try_get::<String, _>("address")
                .context("missing reverse claim address")?;
            let namespace = row
                .try_get::<String, _>("namespace")
                .context("missing reverse claim namespace")?;
            let coin_type = row
                .try_get::<String, _>("coin_type")
                .context("missing reverse claim coin_type")?;
            let reverse_name = row
                .try_get::<String, _>("reverse_name")
                .context("missing reverse claim reverse_name")?;

            Ok((
                reverse_node.clone(),
                ReverseClaimSource {
                    address,
                    namespace,
                    coin_type,
                    reverse_name,
                    reverse_node,
                    claim_provenance: ReverseClaimProvenance {
                        source_family: row
                            .try_get::<String, _>("claim_source_family")
                            .context("missing reverse claim source_family")?,
                        contract_role: row
                            .try_get::<String, _>("claim_contract_role")
                            .context("missing reverse claim contract_role")?,
                        contract_instance_id: row
                            .try_get("claim_contract_instance_id")
                            .context("missing reverse claim contract_instance_id column")?,
                        emitting_address: row
                            .try_get("claim_emitting_address")
                            .context("missing reverse claim emitting_address column")?,
                    },
                },
            ))
        })
        .collect()
}

async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(Some(NameSurface {
            logical_name_id: existing.logical_name_id,
            namespace: existing.namespace,
            input_name: existing.input_name,
            canonical_display_name: existing.canonical_display_name,
            normalized_name: existing.normalized_name,
            dns_encoded_name: existing.dns_encoded_name,
            namehash: existing.namehash,
            labelhashes: existing.labelhashes,
            normalizer_version: existing.normalizer_version,
            normalization_warnings: existing.normalization_warnings,
            normalization_errors: existing.normalization_errors,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: json!({
                "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        }));
    }

    Ok(Some(NameSurface {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        input_name: name.input_name.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        dns_encoded_name: name.dns_encoded_name.clone(),
        namehash: name.namehash.clone(),
        labelhashes: name.labelhashes.clone(),
        normalizer_version: name.normalizer_version.clone(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: reference.chain_id.clone(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "logical_name_id": name.logical_name_id,
            "source_event": "registrar_name_observation",
        }),
        canonicality_state: reference.canonicality_state,
    }))
}

async fn build_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    reference: &ObservationRef,
    provenance: serde_json::Value,
) -> Result<TokenLineage> {
    if let Some(existing) =
        load_token_lineage_including_noncanonical(pool, token_lineage_id).await?
    {
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

async fn build_resource(
    pool: &PgPool,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    chain: &str,
    reference: &BoundaryRef,
    provenance: serde_json::Value,
) -> Result<Resource> {
    if let Some(existing) = load_resource_including_noncanonical(pool, resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(token_lineage_id),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id,
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

async fn build_surface_binding(
    pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, segment.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: segment.active_to.or(existing.active_to),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: segment.anchor_ref.canonicality_state,
        });
    }

    Ok(SurfaceBinding {
        surface_binding_id: segment.surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: segment.authority.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: segment.active_from,
        active_to: segment.active_to,
        chain_id: chain.to_owned(),
        block_hash: segment.anchor_ref.block_hash.clone(),
        block_number: segment.anchor_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": segment.authority.kind.as_str(),
            "authority_key": segment.authority.authority_key,
        }),
        canonicality_state: segment.anchor_ref.canonicality_state,
    })
}

async fn apply_observation(
    history: &mut NameHistory,
    observation: AuthorityObservation,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    match observation {
        AuthorityObservation::RegistrationGranted(event) => {
            let name = observe_registrar_name_with_reference(
                &event.label,
                &event.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            history
                .first_name_ref
                .get_or_insert(event.reference.clone());
            history.name = Some(name.clone());
            history.latest_registry_owner_before_registration =
                history.latest_registry_owner_ref.clone();

            let before_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            let authority_key = format!(
                "registrar:{}:{}:{}:{}:{}",
                event.reference.chain_id,
                event.reference.source_manifest_id,
                event.labelhash,
                event.reference.block_hash,
                event.reference.log_index.unwrap_or_default()
            );
            let lease = RegistrationLease {
                authority_key,
                labelhash: event.labelhash.clone(),
                registrant: event.registrant.clone(),
                expiry: event.expiry,
                release_ref: block_index.first_block_at_or_after(
                    release_after_grace(event.expiry)?,
                    &event.reference.namespace,
                ),
                start_ref: event.reference.clone(),
            };
            let after_anchor = Some(build_registrar_anchor(&lease));
            let before_expiry = history
                .current_registration
                .as_ref()
                .map(|value| value.expiry);
            history.current_registration = Some(lease.clone());

            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                after_anchor.as_ref().map(|value| value.resource_id),
                EVENT_KIND_REGISTRATION_GRANTED,
                json!({
                    "authority_kind": before_anchor.as_ref().map(|value| value.kind.as_str()),
                    "registrant": before_anchor.as_ref().and_then(|value| value.token_lineage_id).map(|_| serde_json::Value::Null),
                }),
                json!({
                    "authority_kind": "registrar",
                    "authority_key": lease.authority_key,
                    "registrant": event.registrant,
                    "expiry": event.expiry.unix_timestamp(),
                    "labelhash": event.labelhash,
                }),
                format!(
                    "grant:{}:{}:{}",
                    event.reference.block_hash,
                    event.reference.transaction_hash.as_deref().unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                after_anchor.as_ref().map(|value| value.resource_id),
                EVENT_KIND_EXPIRY_CHANGED,
                json!({
                    "expiry": before_expiry.map(|value| value.unix_timestamp()),
                }),
                json!({
                    "expiry": event.expiry.unix_timestamp(),
                }),
                format!(
                    "expiry:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            if let (Some(anchor), Some(subject)) = (
                after_anchor.as_ref(),
                nonzero_address(Some(event.registrant.as_str())),
            ) {
                emit_observation_permission_grants(
                    &mut history.events,
                    &event.reference,
                    &name.logical_name_id,
                    anchor,
                    &subject,
                    history.current_resolver.as_deref(),
                    EVENT_KIND_REGISTRATION_GRANTED,
                );
            }

            transition_authority(
                history,
                before_anchor,
                after_anchor,
                &event.reference.as_boundary_ref(),
                event.reference.block_timestamp,
            )?;
        }
        AuthorityObservation::RegistrationRenewed(event) => {
            if history.name.is_none() {
                history.name = Some(observe_registrar_name_with_reference(
                    &event.label,
                    &event.reference,
                    ENS_NORMALIZER_VERSION,
                )?);
                history
                    .first_name_ref
                    .get_or_insert(event.reference.clone());
                let name = history
                    .name
                    .clone()
                    .context("failed to build registrar name metadata")?;
                let lease = RegistrationLease {
                    authority_key: format!(
                        "registrar:{}:{}:{}:{}:{}",
                        event.reference.chain_id,
                        event.reference.source_manifest_id,
                        event.labelhash,
                        event.reference.block_hash,
                        event.reference.log_index.unwrap_or_default()
                    ),
                    labelhash: event.labelhash.clone(),
                    registrant: history
                        .current_registration
                        .as_ref()
                        .map(|value| value.registrant.clone())
                        .unwrap_or_else(|| ZERO_ADDRESS.to_owned()),
                    expiry: event.expiry,
                    release_ref: block_index.first_block_at_or_after(
                        release_after_grace(event.expiry)?,
                        &event.reference.namespace,
                    ),
                    start_ref: event.reference.clone(),
                };
                history.current_registration = Some(lease.clone());
                let anchor = Some(build_registrar_anchor(&lease));
                transition_authority(
                    history,
                    None,
                    anchor.clone(),
                    &event.reference.as_boundary_ref(),
                    event.reference.block_timestamp,
                )?;
                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    anchor.as_ref().map(|value| value.resource_id),
                    EVENT_KIND_REGISTRATION_GRANTED,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "registrant": lease.registrant,
                        "expiry": event.expiry.unix_timestamp(),
                        "labelhash": event.labelhash,
                    }),
                    format!(
                        "grant:{}:{}:{}",
                        event.reference.block_hash,
                        event
                            .reference
                            .transaction_hash
                            .as_deref()
                            .unwrap_or_default(),
                        event.reference.log_index.unwrap_or_default()
                    ),
                ));
                if let (Some(anchor), Some(subject)) = (
                    anchor.as_ref(),
                    nonzero_address(Some(lease.registrant.as_str())),
                ) {
                    emit_observation_permission_grants(
                        &mut history.events,
                        &event.reference,
                        &name.logical_name_id,
                        anchor,
                        &subject,
                        history.current_resolver.as_deref(),
                        EVENT_KIND_REGISTRATION_GRANTED,
                    );
                }
            }
            let name = history
                .name
                .clone()
                .context("failed to build registrar name metadata")?;

            if let Some(current_registration) = history.current_registration.as_mut() {
                let before_expiry = current_registration.expiry;
                current_registration.expiry = event.expiry;
                current_registration.release_ref = block_index.first_block_at_or_after(
                    release_after_grace(event.expiry)?,
                    &event.reference.namespace,
                );

                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    Some(deterministic_uuid(&format!(
                        "resource:{}",
                        current_registration.authority_key
                    ))),
                    EVENT_KIND_REGISTRATION_RENEWED,
                    json!({
                        "expiry": before_expiry.unix_timestamp(),
                    }),
                    json!({
                        "expiry": event.expiry.unix_timestamp(),
                        "labelhash": event.labelhash,
                    }),
                    format!(
                        "renewal:{}:{}:{}",
                        event.reference.block_hash,
                        event
                            .reference
                            .transaction_hash
                            .as_deref()
                            .unwrap_or_default(),
                        event.reference.log_index.unwrap_or_default()
                    ),
                ));
                history.events.push(build_normalized_event(
                    &event.reference,
                    Some(name.logical_name_id.clone()),
                    Some(deterministic_uuid(&format!(
                        "resource:{}",
                        current_registration.authority_key
                    ))),
                    EVENT_KIND_EXPIRY_CHANGED,
                    json!({
                        "expiry": before_expiry.unix_timestamp(),
                    }),
                    json!({
                        "expiry": event.expiry.unix_timestamp(),
                    }),
                    format!(
                        "expiry:{}:{}:{}",
                        event.reference.block_hash,
                        event
                            .reference
                            .transaction_hash
                            .as_deref()
                            .unwrap_or_default(),
                        event.reference.log_index.unwrap_or_default()
                    ),
                ));
            }
        }
        AuthorityObservation::TokenTransferred(event) => {
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            let current_resolver = history.current_resolver.clone();
            let Some(current_registration) = history.current_registration.as_mut() else {
                return Ok(());
            };
            if event.from_address == ZERO_ADDRESS || event.to_address == ZERO_ADDRESS {
                return Ok(());
            }
            let previous_registrant = current_registration.registrant.clone();
            current_registration.registrant = event.to_address.clone();
            let anchor = build_registrar_anchor(current_registration);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(anchor.resource_id),
                EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
                json!({
                    "from": previous_registrant,
                }),
                json!({
                    "to": event.to_address,
                    "labelhash": event.labelhash,
                }),
                format!(
                    "token-transfer:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            emit_observation_permission_subject_change(
                &mut history.events,
                &event.reference,
                &name.logical_name_id,
                &anchor,
                Some(previous_registrant.as_str()),
                Some(event.to_address.as_str()),
                current_resolver.as_deref(),
                EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
            );
        }
        AuthorityObservation::ResolverChanged(event) => {
            let before_resolver = history.current_resolver.clone();
            let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
            let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
            if before_normalized_resolver != after_normalized_resolver {
                history.current_record_version = None;
            }
            history.current_resolver = Some(event.resolver.clone());

            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            let authority = active_anchor_for_history(history, &event.reference.chain_id);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                authority.as_ref().map(|value| value.resource_id),
                EVENT_KIND_RESOLVER_CHANGED,
                json!({
                    "resolver": before_resolver,
                }),
                resolver_changed_after_state(&event, None),
                format!(
                    "resolver:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
            let authority_subject = match authority.as_ref().map(|value| value.kind) {
                Some(AuthorityKind::Registrar) => history
                    .current_registration
                    .as_ref()
                    .map(|registration| registration.registrant.as_str()),
                Some(AuthorityKind::RegistryOnly) => history.current_registry_owner.as_deref(),
                None => None,
            };
            if let (Some(anchor), Some(subject)) =
                (authority.as_ref(), nonzero_address(authority_subject))
            {
                let before_resolver = before_normalized_resolver;
                let after_resolver = after_normalized_resolver;
                if before_resolver != after_resolver {
                    if let Some(previous_resolver) = before_resolver.as_deref() {
                        history
                            .events
                            .push(build_observation_permission_change_event(
                                &event.reference,
                                &name.logical_name_id,
                                anchor,
                                &subject,
                                resolver_permission_scope(
                                    &event.reference.chain_id,
                                    previous_resolver,
                                ),
                                format!("resolver:{previous_resolver}"),
                                PERMISSION_POWER_RESOLVER_CONTROL,
                                PermissionAction::Revoke,
                                EVENT_KIND_RESOLVER_CHANGED,
                            ));
                    }
                    if let Some(current_resolver) = after_resolver.as_deref() {
                        history
                            .events
                            .push(build_observation_permission_change_event(
                                &event.reference,
                                &name.logical_name_id,
                                anchor,
                                &subject,
                                resolver_permission_scope(
                                    &event.reference.chain_id,
                                    current_resolver,
                                ),
                                format!("resolver:{current_resolver}"),
                                PERMISSION_POWER_RESOLVER_CONTROL,
                                PermissionAction::Grant,
                                EVENT_KIND_RESOLVER_CHANGED,
                            ));
                    }
                }
            }
        }
        AuthorityObservation::RecordChanged(event) => {
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            if !current_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let Some(authority) = active_anchor_for_history(history, &event.reference.chain_id)
            else {
                return Ok(());
            };
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(authority.resource_id),
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                record_changed_after_state(&event, None),
                format!(
                    "record-change:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordVersionChanged(event) => {
            let Some(name) = history.name.clone() else {
                return Ok(());
            };
            if !current_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let Some(authority) = active_anchor_for_history(history, &event.reference.chain_id)
            else {
                return Ok(());
            };
            let before_version = history.current_record_version;
            history.current_record_version = Some(event.record_version);
            history.events.push(build_normalized_event(
                &event.reference,
                Some(name.logical_name_id.clone()),
                Some(authority.resource_id),
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({
                    "record_version": before_version,
                }),
                record_version_changed_after_state(&event, None),
                format!(
                    "record-version:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RegistryOwnerChanged(event) => {
            let before_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            let before_owner = history.current_registry_owner.clone();
            history.current_registry_owner = Some(event.owner.clone());
            history.latest_registry_owner_ref = Some(event.reference.clone());
            history
                .registry_resource_anchor
                .get_or_insert_with(|| event.reference.as_boundary_ref());

            let after_anchor = active_anchor_for_history(history, &event.reference.chain_id);
            if matches!(
                (&before_anchor, &after_anchor),
                (Some(left), Some(right))
                    if left.kind == AuthorityKind::RegistryOnly
                        && right.kind == AuthorityKind::RegistryOnly
                        && before_owner != history.current_registry_owner
            ) {
                if let Some(name) = history.name.as_ref() {
                    history.events.push(build_normalized_event(
                        &event.reference,
                        Some(name.logical_name_id.clone()),
                        after_anchor.as_ref().map(|value| value.resource_id),
                        EVENT_KIND_AUTHORITY_TRANSFERRED,
                        json!({
                            "owner": before_owner,
                        }),
                        json!({
                            "owner": history.current_registry_owner,
                            "labelhash": event.labelhash,
                        }),
                        format!(
                            "registry-transfer:{}:{}:{}",
                            event.reference.block_hash,
                            event
                                .reference
                                .transaction_hash
                                .as_deref()
                                .unwrap_or_default(),
                            event.reference.log_index.unwrap_or_default()
                        ),
                    ));
                }
            }
            if let Some(name) = history.name.clone() {
                match (before_anchor.as_ref(), after_anchor.as_ref()) {
                    (Some(before), Some(after))
                        if before.kind == AuthorityKind::RegistryOnly
                            && after.kind == AuthorityKind::RegistryOnly =>
                    {
                        emit_observation_permission_subject_change(
                            &mut history.events,
                            &event.reference,
                            &name.logical_name_id,
                            after,
                            before_owner.as_deref(),
                            history.current_registry_owner.as_deref(),
                            history.current_resolver.as_deref(),
                            EVENT_KIND_AUTHORITY_TRANSFERRED,
                        );
                    }
                    (_, Some(after)) if after.kind == AuthorityKind::RegistryOnly => {
                        if let Some(subject) =
                            nonzero_address(history.current_registry_owner.as_deref())
                        {
                            emit_observation_permission_grants(
                                &mut history.events,
                                &event.reference,
                                &name.logical_name_id,
                                after,
                                &subject,
                                history.current_resolver.as_deref(),
                                EVENT_KIND_AUTHORITY_TRANSFERRED,
                            );
                        }
                    }
                    _ => {}
                }
            }
            transition_authority(
                history,
                before_anchor,
                after_anchor,
                &event.reference.as_boundary_ref(),
                event.reference.block_timestamp,
            )?;
        }
    }

    Ok(())
}

fn apply_reverse_claim_source_observation(
    history: &mut ReverseClaimSourceHistory,
    observation: AuthorityObservation,
) -> Result<()> {
    match observation {
        AuthorityObservation::ResolverChanged(event) => {
            let before_resolver = history.current_resolver.clone();
            let before_normalized_resolver = nonzero_address(before_resolver.as_deref());
            let after_normalized_resolver = nonzero_address(Some(event.resolver.as_str()));
            if before_normalized_resolver != after_normalized_resolver {
                history.current_record_version = None;
            }
            history.current_resolver = Some(event.resolver.clone());
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RESOLVER_CHANGED,
                json!({
                    "resolver": before_resolver,
                }),
                resolver_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "resolver:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            if event.selector.record_key != "name" {
                return Ok(());
            }
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                record_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "record-change:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RecordVersionChanged(event) => {
            if !current_reverse_source_resolver_matches(history, &event.resolver) {
                return Ok(());
            }
            let before_version = history.current_record_version;
            history.current_record_version = Some(event.record_version);
            history.events.push(build_normalized_event(
                &event.reference,
                None,
                None,
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({
                    "record_version": before_version,
                }),
                record_version_changed_after_state(&event, Some(&history.claim_source)),
                format!(
                    "record-version:{}:{}:{}",
                    event.reference.block_hash,
                    event
                        .reference
                        .transaction_hash
                        .as_deref()
                        .unwrap_or_default(),
                    event.reference.log_index.unwrap_or_default()
                ),
            ));
        }
        AuthorityObservation::RegistrationGranted(_)
        | AuthorityObservation::RegistrationRenewed(_)
        | AuthorityObservation::TokenTransferred(_)
        | AuthorityObservation::RegistryOwnerChanged(_) => {}
    }

    Ok(())
}

fn current_reverse_source_resolver_matches(
    history: &ReverseClaimSourceHistory,
    observed_resolver: &str,
) -> bool {
    match (
        nonzero_address(history.current_resolver.as_deref()),
        nonzero_address(Some(observed_resolver)),
    ) {
        (Some(current), Some(observed)) => current == observed,
        _ => false,
    }
}

fn resolver_changed_after_state(
    event: &ResolverObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        ("resolver".to_owned(), Value::String(event.resolver.clone())),
        ("namehash".to_owned(), Value::String(event.namehash.clone())),
    ]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn record_changed_after_state(
    event: &RecordChangeObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([
        (
            "record_key".to_owned(),
            Value::String(event.selector.record_key.clone()),
        ),
        (
            "record_family".to_owned(),
            Value::String(event.selector.record_family.clone()),
        ),
        (
            "selector_key".to_owned(),
            event
                .selector
                .selector_key
                .as_ref()
                .map(|value| Value::String(value.clone()))
                .unwrap_or(Value::Null),
        ),
    ]);
    if let Some(raw_name) = event.raw_name.as_ref() {
        state.insert("raw_name".to_owned(), Value::String(raw_name.clone()));
    }
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn record_version_changed_after_state(
    event: &RecordVersionObservation,
    claim_source: Option<&ReverseClaimSource>,
) -> Value {
    let mut state = Map::from_iter([(
        "record_version".to_owned(),
        Value::Number(event.record_version.into()),
    )]);
    if let Some(claim_source) = claim_source {
        state.insert("primary_claim_source".to_owned(), claim_source.as_value());
    }
    Value::Object(state)
}

fn transition_authority(
    history: &mut NameHistory,
    before: Option<AuthorityAnchor>,
    after: Option<AuthorityAnchor>,
    reference: &BoundaryRef,
    effective_time: OffsetDateTime,
) -> Result<()> {
    if authority_eq(before.as_ref(), after.as_ref()) {
        return Ok(());
    }

    history.current_record_version = None;

    if let Some(open_binding) = history.open_binding.take()
        && open_binding.active_from < effective_time
    {
        history.bindings.push(BindingSegment {
            surface_binding_id: open_binding.surface_binding_id,
            authority: open_binding.authority.clone(),
            active_from: open_binding.active_from,
            active_to: Some(effective_time),
            anchor_ref: open_binding.anchor_ref.clone(),
        });
        if let Some(name) = history.name.as_ref() {
            history.events.push(build_boundary_event(
                reference,
                Some(name.logical_name_id.clone()),
                Some(open_binding.authority.resource_id),
                EVENT_KIND_SURFACE_UNBOUND,
                json!({
                    "authority_kind": open_binding.authority.kind.as_str(),
                    "authority_key": open_binding.authority.authority_key,
                }),
                json!({
                    "authority_kind": open_binding.authority.kind.as_str(),
                    "authority_key": open_binding.authority.authority_key,
                    "active_to": effective_time.unix_timestamp(),
                }),
                format!(
                    "surface-unbound:{}:{}:{}",
                    reference.block_hash, name.logical_name_id, open_binding.surface_binding_id
                ),
                open_binding.authority.binding_source_family.clone(),
                open_binding.authority.binding_manifest_version,
                Some(open_binding.authority.binding_manifest_id),
                reference.canonicality_state,
            ));
        }
    }

    if let Some(after_anchor) = after.clone() {
        let surface_binding_id = deterministic_uuid(&format!(
            "binding:{}:{}",
            after_anchor.authority_key,
            effective_time.unix_timestamp()
        ));
        history.open_binding = Some(OpenBinding {
            surface_binding_id,
            authority: after_anchor.clone(),
            active_from: effective_time,
            anchor_ref: reference.clone(),
        });
        if let Some(name) = history.name.as_ref() {
            history.events.push(build_boundary_event(
                reference,
                Some(name.logical_name_id.clone()),
                Some(after_anchor.resource_id),
                EVENT_KIND_SURFACE_BOUND,
                json!({}),
                json!({
                    "authority_kind": after_anchor.kind.as_str(),
                    "authority_key": after_anchor.authority_key,
                    "active_from": effective_time.unix_timestamp(),
                    "binding_kind": SurfaceBindingKind::DeclaredRegistryPath.as_str(),
                }),
                format!(
                    "surface-bound:{}:{}:{}",
                    reference.block_hash, name.logical_name_id, surface_binding_id
                ),
                after_anchor.binding_source_family.clone(),
                after_anchor.binding_manifest_version,
                Some(after_anchor.binding_manifest_id),
                reference.canonicality_state,
            ));
        }
    }

    if let Some(name) = history.name.as_ref() {
        let source_family = after
            .as_ref()
            .map(|value| value.binding_source_family.clone())
            .or_else(|| {
                before
                    .as_ref()
                    .map(|value| value.binding_source_family.clone())
            })
            .unwrap_or_else(|| default_registrar_source_family(&name.namespace).to_owned());
        let manifest_version = after
            .as_ref()
            .map(|value| value.binding_manifest_version)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_version))
            .unwrap_or(1);
        let manifest_id = after
            .as_ref()
            .map(|value| value.binding_manifest_id)
            .or_else(|| before.as_ref().map(|value| value.binding_manifest_id))
            .unwrap_or(0);
        history.events.push(build_boundary_event(
            reference,
            Some(name.logical_name_id.clone()),
            after
                .as_ref()
                .map(|value| value.resource_id)
                .or(before.as_ref().map(|value| value.resource_id)),
            EVENT_KIND_AUTHORITY_EPOCH_CHANGED,
            json!({
                "authority_kind": before.as_ref().map(|value| value.kind.as_str()),
                "authority_key": before.as_ref().map(|value| value.authority_key.clone()),
            }),
            json!({
                "authority_kind": after.as_ref().map(|value| value.kind.as_str()),
                "authority_key": after.as_ref().map(|value| value.authority_key.clone()),
            }),
            format!(
                "authority-epoch:{}:{}:{}:{}:{}",
                reference.block_hash,
                name.logical_name_id,
                effective_time.unix_timestamp(),
                before
                    .as_ref()
                    .map(|value| value.authority_key.as_str())
                    .unwrap_or("none"),
                after
                    .as_ref()
                    .map(|value| value.authority_key.as_str())
                    .unwrap_or("none")
            ),
            source_family,
            manifest_version,
            Some(manifest_id).filter(|value| *value > 0),
            reference.canonicality_state,
        ));
    }

    Ok(())
}

fn authority_eq(left: Option<&AuthorityAnchor>, right: Option<&AuthorityAnchor>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.authority_key == right.authority_key,
        _ => false,
    }
}

fn active_anchor_for_history(history: &NameHistory, chain: &str) -> Option<AuthorityAnchor> {
    if let Some(registration) = history.current_registration.as_ref() {
        return Some(build_registrar_anchor(registration));
    }
    registry_anchor_for_history(history, chain, &history.labelhash)
}

fn current_resolver_matches(history: &NameHistory, resolver: &str) -> bool {
    nonzero_address(history.current_resolver.as_deref())
        .is_some_and(|current| current.eq_ignore_ascii_case(resolver))
}

fn nonzero_address(value: Option<&str>) -> Option<String> {
    value
        .filter(|address| !address.eq_ignore_ascii_case(ZERO_ADDRESS))
        .map(ToOwned::to_owned)
}

fn resource_permission_scope() -> serde_json::Value {
    json!({
        "kind": "resource",
    })
}

fn resolver_permission_scope(chain_id: &str, resolver: &str) -> serde_json::Value {
    json!({
        "kind": "resolver",
        "chain_id": chain_id,
        "resolver_address": resolver,
    })
}

fn permission_source(anchor: &AuthorityAnchor, source_event_kind: &str) -> serde_json::Value {
    json!({
        "kind": "ens_v1_authority",
        "authority_kind": anchor.kind.as_str(),
        "authority_key": anchor.authority_key,
        "source_event_kind": source_event_kind,
    })
}

fn permission_state(
    subject: &str,
    scope: serde_json::Value,
    effective_powers: &[&str],
    grant_source: Option<serde_json::Value>,
    revocation_source: Option<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "subject": subject,
        "scope": scope,
        "effective_powers": effective_powers,
        "grant_source": grant_source,
        "revocation_source": revocation_source,
        "inheritance_path": [],
        "transfer_behavior": PERMISSION_TRANSFER_BEHAVIOR,
    })
}

fn build_observation_permission_change_event(
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    scope: serde_json::Value,
    scope_identity: String,
    power: &str,
    action: PermissionAction,
    source_event_kind: &str,
) -> NormalizedEvent {
    let source = permission_source(anchor, source_event_kind);
    let before_state = match action {
        PermissionAction::Grant => permission_state(subject, scope.clone(), &[], None, None),
        PermissionAction::Revoke => {
            permission_state(subject, scope.clone(), &[power], Some(source.clone()), None)
        }
    };
    let after_state = match action {
        PermissionAction::Grant => permission_state(subject, scope, &[power], Some(source), None),
        PermissionAction::Revoke => permission_state(subject, scope, &[], None, Some(source)),
    };

    build_normalized_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_PERMISSION_CHANGED,
        before_state,
        after_state,
        format!(
            "permission:{}:{}:{}:{}:{}:{}",
            action.as_str(),
            scope_identity,
            subject,
            reference.block_hash,
            reference.transaction_hash.as_deref().unwrap_or_default(),
            reference.log_index.unwrap_or_default()
        ),
    )
}

fn build_boundary_permission_change_event(
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    scope: serde_json::Value,
    scope_identity: String,
    power: &str,
    action: PermissionAction,
    source_event_kind: &str,
) -> NormalizedEvent {
    let source = permission_source(anchor, source_event_kind);
    let before_state = match action {
        PermissionAction::Grant => permission_state(subject, scope.clone(), &[], None, None),
        PermissionAction::Revoke => {
            permission_state(subject, scope.clone(), &[power], Some(source.clone()), None)
        }
    };
    let after_state = match action {
        PermissionAction::Grant => permission_state(subject, scope, &[power], Some(source), None),
        PermissionAction::Revoke => permission_state(subject, scope, &[], None, Some(source)),
    };

    build_boundary_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_PERMISSION_CHANGED,
        before_state,
        after_state,
        format!(
            "permission:{}:{}:{}:{}:{}",
            action.as_str(),
            scope_identity,
            subject,
            reference.block_hash,
            anchor.authority_key
        ),
        anchor.binding_source_family.clone(),
        anchor.binding_manifest_version,
        Some(anchor.binding_manifest_id),
        reference.canonicality_state,
    )
}

fn emit_observation_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    events.push(build_observation_permission_change_event(
        reference,
        logical_name_id,
        anchor,
        subject,
        resource_permission_scope(),
        "resource".to_owned(),
        PERMISSION_POWER_RESOURCE_CONTROL,
        PermissionAction::Grant,
        source_event_kind,
    ));

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(build_observation_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver_permission_scope(&reference.chain_id, &resolver),
            format!("resolver:{resolver}"),
            PERMISSION_POWER_RESOLVER_CONTROL,
            PermissionAction::Grant,
            source_event_kind,
        ));
    }
}

fn emit_boundary_permission_grants(
    events: &mut Vec<NormalizedEvent>,
    reference: &BoundaryRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    subject: &str,
    resolver: Option<&str>,
    chain_id: &str,
    source_event_kind: &str,
) {
    events.push(build_boundary_permission_change_event(
        reference,
        logical_name_id,
        anchor,
        subject,
        resource_permission_scope(),
        "resource".to_owned(),
        PERMISSION_POWER_RESOURCE_CONTROL,
        PermissionAction::Grant,
        source_event_kind,
    ));

    if let Some(resolver) = nonzero_address(resolver) {
        events.push(build_boundary_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver_permission_scope(chain_id, &resolver),
            format!("resolver:{resolver}"),
            PERMISSION_POWER_RESOLVER_CONTROL,
            PermissionAction::Grant,
            source_event_kind,
        ));
    }
}

fn emit_observation_permission_subject_change(
    events: &mut Vec<NormalizedEvent>,
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    before_subject: Option<&str>,
    after_subject: Option<&str>,
    resolver: Option<&str>,
    source_event_kind: &str,
) {
    let before_subject = nonzero_address(before_subject);
    let after_subject = nonzero_address(after_subject);
    if before_subject == after_subject {
        return;
    }

    if let Some(subject) = before_subject.as_deref() {
        events.push(build_observation_permission_change_event(
            reference,
            logical_name_id,
            anchor,
            subject,
            resource_permission_scope(),
            "resource".to_owned(),
            PERMISSION_POWER_RESOURCE_CONTROL,
            PermissionAction::Revoke,
            source_event_kind,
        ));
        if let Some(resolver) = nonzero_address(resolver) {
            events.push(build_observation_permission_change_event(
                reference,
                logical_name_id,
                anchor,
                subject,
                resolver_permission_scope(&reference.chain_id, &resolver),
                format!("resolver:{resolver}"),
                PERMISSION_POWER_RESOLVER_CONTROL,
                PermissionAction::Revoke,
                source_event_kind,
            ));
        }
    }

    if let Some(subject) = after_subject.as_deref() {
        emit_observation_permission_grants(
            events,
            reference,
            logical_name_id,
            anchor,
            subject,
            resolver,
            source_event_kind,
        );
    }
}

fn emit_registration_released_event(
    history: &mut NameHistory,
    lease: &RegistrationLease,
    release_ref: &BoundaryRef,
) -> Result<()> {
    let Some(name) = history.name.as_ref() else {
        return Ok(());
    };
    history.events.push(build_boundary_event(
        release_ref,
        Some(name.logical_name_id.clone()),
        Some(deterministic_uuid(&format!(
            "resource:{}",
            lease.authority_key
        ))),
        EVENT_KIND_REGISTRATION_RELEASED,
        json!({
            "registrant": lease.registrant,
            "expiry": lease.expiry.unix_timestamp(),
        }),
        json!({
            "released_at": release_ref.block_timestamp.unix_timestamp(),
            "labelhash": lease.labelhash,
        }),
        format!(
            "release:{}:{}:{}",
            release_ref.block_hash, name.logical_name_id, lease.authority_key
        ),
        lease.start_ref.source_family.clone(),
        lease.start_ref.manifest_version,
        Some(lease.start_ref.source_manifest_id),
        release_ref.canonicality_state,
    ));
    Ok(())
}

impl RegistrationLease {
    fn reference_chain(&self) -> String {
        self.start_ref.chain_id.clone()
    }
}

impl ObservationRef {
    fn as_boundary_ref(&self) -> BoundaryRef {
        BoundaryRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
        }
    }
}

fn release_after_grace(expiry: OffsetDateTime) -> Result<OffsetDateTime> {
    let release_unix = expiry
        .unix_timestamp()
        .checked_add(ENS_GRACE_PERIOD_SECS)
        .context("ENSv1 release timestamp overflowed i64")?;
    OffsetDateTime::from_unix_timestamp(release_unix)
        .context("ENSv1 release timestamp is not a valid unix timestamp")
}

fn build_normalized_event(
    reference: &ObservationRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: serde_json::Value,
    after_state: serde_json::Value,
    identity_suffix: String,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, event_kind, identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: reference.source_family.clone(),
        manifest_version: reference.manifest_version,
        source_manifest_id: Some(reference.source_manifest_id),
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: reference.transaction_hash.clone(),
        log_index: reference.log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "transaction_hash": reference.transaction_hash,
            "transaction_index": reference.transaction_index,
            "log_index": reference.log_index,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: reference.canonicality_state,
        before_state,
        after_state,
    }
}

fn build_boundary_event(
    reference: &BoundaryRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: serde_json::Value,
    after_state: serde_json::Value,
    identity_suffix: String,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY, event_kind, identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family,
        manifest_version,
        source_manifest_id,
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "kind": "raw_block",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "block_timestamp": reference.block_timestamp.unix_timestamp(),
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state,
        before_state,
        after_state,
    }
}

fn observation_labelhash(observation: &AuthorityObservation) -> String {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => value.labelhash.clone(),
        AuthorityObservation::RegistrationRenewed(value) => value.labelhash.clone(),
        AuthorityObservation::TokenTransferred(value) => value.labelhash.clone(),
        AuthorityObservation::RegistryOwnerChanged(value) => value.labelhash.clone(),
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_) => {
            unreachable!("resolver observations must be resolved by namehash before use")
        }
    }
}

fn observation_namehash(observation: &AuthorityObservation) -> Option<&str> {
    match observation {
        AuthorityObservation::ResolverChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordChanged(value) => Some(&value.namehash),
        AuthorityObservation::RecordVersionChanged(value) => Some(&value.namehash),
        _ => None,
    }
}

fn build_authority_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let profile = authority_profile_for_source_family(&raw_log.source_family);

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && topic0.eq_ignore_ascii_case(&name_registered_topic0())
    {
        let label = decode_first_dynamic_string(&raw_log.data)?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered log is missing indexed labelhash")?,
        )?;
        let observed = profile
            .context("registrar observation is missing an authority profile")?
            .observe_name(&label, &raw_log.normalizer_version)?;
        let observed_labelhash = observed
            .labelhashes
            .first()
            .context("observed registrar name is missing labelhash")?;
        if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
            bail!("NameRegistered labelhash does not match decoded label");
        }
        let registrant = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("NameRegistered log is missing indexed owner")?,
        )?;
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(64..96)
                .context("NameRegistered data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::RegistrationGranted(
            NameRegistrationObservation {
                label,
                labelhash,
                registrant,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("NameRegistered expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && topic0.eq_ignore_ascii_case(&name_renewed_topic0())
    {
        let label = decode_first_dynamic_string(&raw_log.data)?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed log is missing indexed labelhash")?,
        )?;
        let observed = profile
            .context("registrar renewal observation is missing an authority profile")?
            .observe_name(&label, &raw_log.normalizer_version)?;
        let observed_labelhash = observed
            .labelhashes
            .first()
            .context("observed renewed registrar name is missing labelhash")?;
        if !observed_labelhash.eq_ignore_ascii_case(&labelhash) {
            bail!("NameRenewed labelhash does not match decoded label");
        }
        let expiry = abi_word_to_i64(
            raw_log
                .data
                .get(64..96)
                .context("NameRenewed data is missing expiry word")?,
        )?;
        return Ok(Some(AuthorityObservation::RegistrationRenewed(
            NameRenewalObservation {
                label,
                labelhash,
                expiry: OffsetDateTime::from_unix_timestamp(expiry)
                    .context("NameRenewed expiry is not a valid unix timestamp")?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registrar_source_family())
        && topic0.eq_ignore_ascii_case(&transfer_topic0())
    {
        if raw_log.topics.len() < 4 {
            bail!("Transfer log is missing indexed topics");
        }
        return Ok(Some(AuthorityObservation::TokenTransferred(
            TokenTransferObservation {
                labelhash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(3)
                        .context("Transfer topic3 is missing token id")?,
                )?,
                from_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(1)
                        .context("Transfer topic1 is missing from address")?,
                )?,
                to_address: normalize_topic_address(
                    raw_log
                        .topics
                        .get(2)
                        .context("Transfer topic2 is missing to address")?,
                )?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && topic0.eq_ignore_ascii_case(&new_owner_topic0())
    {
        let parent_node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NewOwner log is missing parent node")?,
        )?;
        if parent_node
            != profile
                .context("registry observation is missing an authority profile")?
                .root_node()
        {
            return Ok(None);
        }
        return Ok(Some(AuthorityObservation::RegistryOwnerChanged(
            RegistryOwnerObservation {
                labelhash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(2)
                        .context("NewOwner log is missing indexed labelhash")?,
                )?,
                owner: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.registry_source_family())
        && topic0.eq_ignore_ascii_case(&new_resolver_topic0())
    {
        return Ok(Some(AuthorityObservation::ResolverChanged(
            ResolverObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("NewResolver log is missing indexed node")?,
                )?,
                resolver: decode_owner_address(&raw_log.data)?,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&text_changed_topic0())
    {
        let key = decode_first_dynamic_string(&raw_log.data)?;
        let indexed_key_hash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TextChanged log is missing indexed key hash")?,
        )?;
        if indexed_key_hash != keccak256_hex(key.as_bytes()) {
            bail!("TextChanged indexed key hash does not match decoded key");
        }
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("TextChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: "text".to_owned(),
                    record_family: "text".to_owned(),
                    selector_key: None,
                },
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&name_changed_topic0())
    {
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("NameChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: "name".to_owned(),
                    record_family: "name".to_owned(),
                    selector_key: None,
                },
                raw_name: Some(decode_first_dynamic_string(&raw_log.data)?),
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&addr_changed_topic0())
    {
        decode_owner_address(&raw_log.data)?;
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("AddrChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: format!("addr:{ENS_NATIVE_COIN_TYPE}"),
                    record_family: "addr".to_owned(),
                    selector_key: Some(ENS_NATIVE_COIN_TYPE.to_owned()),
                },
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&address_changed_topic0())
    {
        let coin_type = abi_word_to_i64(
            raw_log
                .data
                .get(..32)
                .context("AddressChanged log is missing coin type")?,
        )?;
        decode_nth_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(AuthorityObservation::RecordChanged(
            RecordChangeObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("AddressChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                selector: RecordSelector {
                    record_key: format!("addr:{coin_type}"),
                    record_family: "addr".to_owned(),
                    selector_key: Some(coin_type.to_string()),
                },
                raw_name: None,
                reference: raw_log.reference(),
            },
        )));
    }

    if matches!(profile, Some(profile) if raw_log.source_family == profile.resolver_source_family())
        && topic0.eq_ignore_ascii_case(&version_changed_topic0())
    {
        return Ok(Some(AuthorityObservation::RecordVersionChanged(
            RecordVersionObservation {
                namehash: normalize_hex_32(
                    raw_log
                        .topics
                        .get(1)
                        .context("VersionChanged log is missing indexed node")?,
                )?,
                resolver: raw_log.emitting_address.clone(),
                record_version: abi_word_to_i64(
                    raw_log
                        .data
                        .get(..32)
                        .context("VersionChanged log is missing record version")?,
                )?,
                reference: raw_log.reference(),
            },
        )));
    }

    Ok(None)
}

impl AuthorityRawLogRow {
    fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: Some(self.transaction_hash.clone()),
            transaction_index: Some(self.transaction_index),
            log_index: Some(self.log_index),
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

impl CanonicalBlockIndex {
    fn first_block_at_or_after(
        &self,
        timestamp: OffsetDateTime,
        namespace: &str,
    ) -> Option<BoundaryRef> {
        self.blocks
            .iter()
            .find(|block| block.block_timestamp >= timestamp)
            .map(|block| BoundaryRef {
                chain_id: block.chain_id.clone(),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                block_timestamp: block.block_timestamp,
                canonicality_state: block.canonicality_state,
                namespace: namespace.to_owned(),
            })
    }
}

async fn load_canonical_blocks(pool: &PgPool, chain: &str) -> Result<Vec<RawBlockSnapshot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load canonical raw blocks for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            Ok(RawBlockSnapshot {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
            })
        })
        .collect()
}

async fn load_authority_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
) -> Result<Vec<AuthorityRawLogRow>> {
    let emitters_by_address = active_emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rb.block_timestamp AS block_timestamp,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN raw_blocks rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load ENSv1 unwrapped authority raw logs for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?
                .to_ascii_lowercase();
            let emitter = emitters_by_address.get(&address).with_context(|| {
                format!("missing active emitter metadata for chain {chain} address {address}")
            })?;
            Ok(AuthorityRawLogRow {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                block_timestamp: row
                    .try_get("block_timestamp")
                    .context("missing block_timestamp")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing transaction_index")?,
                log_index: row.try_get("log_index").context("missing log_index")?,
                emitting_address: address,
                topics: row.try_get("topics").context("missing topics")?,
                data: row.try_get("data").context("missing data")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
                normalizer_version: emitter.normalizer_version.clone(),
            })
        })
        .collect()
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv1 unwrapped authority attribution")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contracts
        .iter()
        .map(|contract| {
            contract.source_manifest_id.with_context(|| {
                format!(
                    "watched contract {} on {} is missing source_manifest_id",
                    contract.address, contract.chain
                )
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;
    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let Some(source_manifest_id) = watched_contract.source_manifest_id else {
            continue;
        };
        let Some(manifest) = active_manifests.get(&source_manifest_id) else {
            continue;
        };
        if manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
        {
            continue;
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            source_rank: source_rank(watched_contract.source),
        };

        match emitters_by_address.get(&candidate.address) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_address.insert(candidate.address.clone(), candidate);
            }
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
    });
    Ok(emitters)
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
                chain: row.try_get("chain").context("missing chain")?,
                namespace: row.try_get("namespace").context("missing namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                manifest_version: row
                    .try_get("manifest_version")
                    .context("missing manifest_version")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("missing normalizer_version")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (candidate.source_rank, candidate.source_manifest_id)
        < (current.source_rank, current.source_manifest_id)
}

fn authority_profile_for_source_family(source_family: &str) -> Option<AuthorityProfile> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
        | SOURCE_FAMILY_ENS_V1_REGISTRY_L1
        | SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => Some(AuthorityProfile::Ens),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(AuthorityProfile::Basenames),
        _ => None,
    }
}

fn observe_registrar_name_with_reference(
    label: &str,
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    authority_profile_for_source_family(&reference.source_family)
        .with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?
        .observe_name(label, normalizer_version)
}

fn observe_registrar_name_with_version(
    label: &str,
    profile: AuthorityProfile,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }
    let normalized_label = label.to_ascii_lowercase();
    let (normalized_name, input_name, parent_labels) = match profile {
        AuthorityProfile::Ens => (
            format!("{normalized_label}.eth"),
            format!("{label}.eth"),
            vec![b"eth".to_vec()],
        ),
        AuthorityProfile::Basenames => (
            format!("{normalized_label}.base.eth"),
            format!("{label}.base.eth"),
            vec![b"base".to_vec(), b"eth".to_vec()],
        ),
    };
    let label_length =
        u8::try_from(normalized_label.len()).context("registrar label exceeds DNS length")?;
    let dns_capacity = 2
        + normalized_label.len()
        + parent_labels
            .iter()
            .map(|label| 1 + label.len())
            .sum::<usize>();
    let mut dns_name = Vec::with_capacity(dns_capacity);
    dns_name.push(label_length);
    dns_name.extend_from_slice(normalized_label.as_bytes());
    for label in &parent_labels {
        dns_name
            .push(u8::try_from(label.len()).context("registrar suffix label exceeds DNS length")?);
        dns_name.extend_from_slice(label);
    }
    dns_name.push(0);
    let mut namehash_labels = Vec::with_capacity(1 + parent_labels.len());
    namehash_labels.push(normalized_label.as_bytes().to_vec());
    namehash_labels.extend(parent_labels.iter().cloned());
    let mut labelhashes = Vec::with_capacity(namehash_labels.len());
    for label in &namehash_labels {
        labelhashes.push(keccak256_hex(label));
    }
    Ok(NameMetadata {
        namespace: profile.namespace().to_owned(),
        logical_name_id: format!("{}:{normalized_name}", profile.namespace()),
        input_name: input_name.clone(),
        canonical_display_name: normalized_name.clone(),
        normalized_name: normalized_name.clone(),
        dns_encoded_name: dns_name.clone(),
        namehash: namehash_hex(&namehash_labels),
        labelhashes,
        normalizer_version: normalizer_version.to_owned(),
    })
}

fn observe_registrar_eth_name_with_version(
    label: &str,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    observe_registrar_name_with_version(label, AuthorityProfile::Ens, normalizer_version)
}

fn decode_first_dynamic_string(data: &[u8]) -> Result<String> {
    String::from_utf8(decode_first_dynamic_bytes(data)?)
        .context("dynamic string payload is not valid UTF-8")
}

fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    decode_nth_dynamic_bytes(data, 0)
}

fn decode_nth_dynamic_bytes(data: &[u8], parameter_index: usize) -> Result<Vec<u8>> {
    let offset_start = parameter_index
        .checked_mul(32)
        .context("dynamic ABI parameter index overflowed")?;
    if data.len() < 64 {
        bail!("event data is too short to decode a dynamic bytes parameter");
    }
    let offset = word_to_usize(
        data.get(offset_start..offset_start + 32)
            .context("event data is missing dynamic bytes offset")?,
    )
    .context("invalid ABI offset")?;
    if data.len() < offset + 32 {
        bail!("event data is missing dynamic bytes length");
    }
    let byte_length = word_to_usize(&data[offset..offset + 32]).context("invalid ABI length")?;
    let bytes_start = offset + 32;
    let bytes_end = bytes_start + byte_length;
    if data.len() < bytes_end {
        bail!("event data does not contain the full dynamic bytes payload");
    }
    Ok(data[bytes_start..bytes_end].to_vec())
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

fn abi_word_to_i64(word: &[u8]) -> Result<i64> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported i64 width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in i64")
}

fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

fn decode_owner_address(data: &[u8]) -> Result<String> {
    let word = data
        .get(..32)
        .context("owner address payload is missing the first ABI word")?;
    let mut output = String::from("0x");
    for byte in &word[12..32] {
        output.push_str(&format!("{byte:02x}"));
    }
    Ok(output)
}

fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_string(&digest)
}

fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut hasher = Keccak256::new();
            hasher.update(label);
            let digest = hasher.finalize();
            let mut output = [0u8; 32];
            output.copy_from_slice(&digest);
            output
        };
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        let mut hasher = Keccak256::new();
        hasher.update(combined);
        node.copy_from_slice(&hasher.finalize());
    }
    hex_string(&node)
}

fn eth_node() -> String {
    namehash_hex(&[b"eth".to_vec()])
}

fn base_eth_node() -> String {
    namehash_hex(&[b"base".to_vec(), b"eth".to_vec()])
}

fn name_registered_topic0() -> String {
    keccak256_hex(NAME_REGISTERED_SIGNATURE.as_bytes())
}

fn name_renewed_topic0() -> String {
    keccak256_hex(NAME_RENEWED_SIGNATURE.as_bytes())
}

fn transfer_topic0() -> String {
    keccak256_hex(TRANSFER_SIGNATURE.as_bytes())
}

fn new_owner_topic0() -> String {
    keccak256_hex(NEW_OWNER_SIGNATURE.as_bytes())
}

fn new_resolver_topic0() -> String {
    keccak256_hex(NEW_RESOLVER_SIGNATURE.as_bytes())
}

fn name_changed_topic0() -> String {
    keccak256_hex(NAME_CHANGED_SIGNATURE.as_bytes())
}

fn addr_changed_topic0() -> String {
    keccak256_hex(ADDR_CHANGED_SIGNATURE.as_bytes())
}

fn address_changed_topic0() -> String {
    keccak256_hex(ADDRESS_CHANGED_SIGNATURE.as_bytes())
}

fn text_changed_topic0() -> String {
    keccak256_hex(TEXT_CHANGED_SIGNATURE.as_bytes())
}

fn version_changed_topic0() -> String {
    keccak256_hex(VERSION_CHANGED_SIGNATURE.as_bytes())
}

fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use bigname_storage::{
        NormalizedEvent, RawBlock, RawLog, default_database_url, load_name_surface,
        load_normalized_event_counts_by_kind, load_surface_bindings_by_logical_name_id,
        upsert_normalized_events, upsert_raw_blocks, upsert_raw_logs,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for ENSv1 unwrapped authority tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_ens_v1_unwrapped_authority_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for ENSv1 unwrapped authority tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for ENSv1 unwrapped authority tests")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for ENSv1 unwrapped authority tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    async fn insert_manifest_version(
        pool: &PgPool,
        manifest_version: i64,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        rollout_status: &str,
        normalizer_version: &str,
        file_path: &str,
    ) -> Result<i64> {
        sqlx::query_scalar(
            r#"
            INSERT INTO manifest_versions (
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
            RETURNING manifest_id
            "#,
        )
        .bind(manifest_version)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind("{}")
        .fetch_one(pool)
        .await
        .context("failed to insert manifest version")
    }

    async fn insert_manifest_contract_instance(
        pool: &PgPool,
        manifest_id: i64,
        declaration_kind: &str,
        declaration_name: &str,
        contract_instance_id: Uuid,
        declared_address: &str,
        role: Option<&str>,
        proxy_kind: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                code_hash,
                abi_ref,
                role,
                proxy_kind,
                implementation_contract_instance_id,
                declared_implementation_address
            )
            VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, NULL, NULL)
            "#,
        )
        .bind(manifest_id)
        .bind(declaration_kind)
        .bind(declaration_name)
        .bind(contract_instance_id)
        .bind(declared_address)
        .bind(role)
        .bind(proxy_kind)
        .execute(pool)
        .await
        .context("failed to insert manifest contract instance")?;
        Ok(())
    }

    async fn insert_contract_instance(
        pool: &PgPool,
        contract_instance_id: Uuid,
        chain_id: &str,
        contract_kind: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, $3, $4::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain_id)
        .bind(contract_kind)
        .bind("{}")
        .execute(pool)
        .await
        .context("failed to insert contract instance")?;
        Ok(())
    }

    async fn insert_contract_instance_address(
        pool: &PgPool,
        contract_instance_id: Uuid,
        chain_id: &str,
        address: &str,
        source_manifest_id: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain_id)
        .bind(address)
        .bind(source_manifest_id)
        .bind("{}")
        .execute(pool)
        .await
        .context("failed to insert contract-instance address")?;
        Ok(())
    }

    async fn insert_active_contract_fixture(
        pool: &PgPool,
        source_family: &str,
        declaration_name: &str,
        address: &str,
        role: Option<&str>,
        file_path: &str,
    ) -> Result<i64> {
        insert_active_contract_fixture_with_manifest(
            pool,
            "ens",
            source_family,
            "ethereum-mainnet",
            "ens_v1",
            declaration_name,
            address,
            role,
            file_path,
        )
        .await
    }

    async fn insert_active_contract_fixture_with_manifest(
        pool: &PgPool,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        declaration_name: &str,
        address: &str,
        role: Option<&str>,
        file_path: &str,
    ) -> Result<i64> {
        let manifest_id = insert_manifest_version(
            pool,
            1,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            "active",
            "uts46-v1",
            file_path,
        )
        .await?;
        let contract_instance_id = Uuid::new_v4();
        insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
        insert_manifest_contract_instance(
            pool,
            manifest_id,
            "contract",
            declaration_name,
            contract_instance_id,
            address,
            role,
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(pool, contract_instance_id, chain, address, manifest_id)
            .await?;
        Ok(manifest_id)
    }

    fn raw_block(
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
        timestamp: i64,
    ) -> RawBlock {
        raw_block_on_chain(
            "ethereum-mainnet",
            block_hash,
            parent_hash,
            block_number,
            timestamp,
        )
    }

    fn raw_block_on_chain(
        chain_id: &str,
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
        timestamp: i64,
    ) -> RawBlock {
        RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: parent_hash.map(str::to_owned),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
                .expect("test block timestamp must be valid"),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }
    }

    fn abi_word_u64(value: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn abi_word_address(address: &str) -> [u8; 32] {
        let normalized = address.trim_start_matches("0x");
        assert_eq!(normalized.len(), 40, "address must be 20 bytes");
        let mut word = [0u8; 32];
        for (index, chunk) in normalized.as_bytes().chunks(2).enumerate() {
            let value = std::str::from_utf8(chunk).expect("hex address chunk must be utf-8");
            word[12 + index] = u8::from_str_radix(value, 16).expect("address must be hex");
        }
        word
    }

    fn encode_registrar_name_registered_log_data(label: &str, expiry_unix: i64) -> Vec<u8> {
        let label_bytes = label.as_bytes();
        let mut output = Vec::new();

        output.extend_from_slice(&abi_word_u64(96));
        output.extend_from_slice(&abi_word_u64(1));
        output.extend_from_slice(&abi_word_u64(expiry_unix as u64));
        output.extend_from_slice(&abi_word_u64(
            u64::try_from(label_bytes.len()).expect("test label length must fit in u64"),
        ));
        output.extend_from_slice(label_bytes);

        let padded_length = ((label_bytes.len() + 31) / 32) * 32;
        output.resize(32 * 4 + padded_length, 0);
        output
    }

    fn encode_registry_new_resolver_log_data(resolver: &str) -> Vec<u8> {
        abi_word_address(resolver).to_vec()
    }

    fn encode_dynamic_string_log_data(value: &str) -> Vec<u8> {
        let value_bytes = value.as_bytes();
        let mut output = Vec::new();
        output.extend_from_slice(&abi_word_u64(32));
        output.extend_from_slice(&abi_word_u64(
            u64::try_from(value_bytes.len()).expect("test string length must fit in u64"),
        ));
        output.extend_from_slice(value_bytes);
        let padded_length = ((value_bytes.len() + 31) / 32) * 32;
        output.resize(64 + padded_length, 0);
        output
    }

    fn encode_resolver_addr_changed_log_data(address: &str) -> Vec<u8> {
        abi_word_address(address).to_vec()
    }

    fn encode_resolver_address_changed_log_data(coin_type: u64, address_bytes: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&abi_word_u64(coin_type));
        output.extend_from_slice(&abi_word_u64(64));
        output.extend_from_slice(&abi_word_u64(
            u64::try_from(address_bytes.len()).expect("test address length must fit in u64"),
        ));
        output.extend_from_slice(address_bytes);
        let padded_length = ((address_bytes.len() + 31) / 32) * 32;
        output.resize(96 + padded_length, 0);
        output
    }

    fn encode_resolver_version_changed_log_data(version: u64) -> Vec<u8> {
        abi_word_u64(version).to_vec()
    }

    fn reverse_claim_event(
        source_manifest_id: i64,
        block_hash: &str,
        transaction_hash: &str,
        log_index: i64,
        claimed_address: &str,
        reverse_node: &str,
        reverse_name: &str,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!(
                "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_REVERSE_CHANGED}:{block_hash}:{transaction_hash}:{log_index}:{claimed_address}"
            ),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
            source_family: "ens_v1_reverse_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(source_manifest_id),
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(42),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(transaction_hash.to_owned()),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": block_hash,
                "block_number": 42,
                "transaction_hash": transaction_hash,
                "transaction_index": 0,
                "log_index": log_index,
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            }),
            derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ReverseClaimed",
                "address": claimed_address,
                "coin_type": ENS_NATIVE_COIN_TYPE,
                "namespace": "ens",
                "reverse_namespace": "ens",
                "reverse_label": claimed_address.trim_start_matches("0x").to_ascii_lowercase(),
                "reverse_name": reverse_name,
                "reverse_node": reverse_node,
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
                    "contract_instance_id": Uuid::from_u128(0x44).to_string(),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        }
    }

    fn basenames_reverse_claim_event(
        source_manifest_id: i64,
        block_hash: &str,
        transaction_hash: &str,
        log_index: i64,
        claimed_address: &str,
        reverse_node: &str,
        reverse_name: &str,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!(
                "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_REVERSE_CHANGED}:{block_hash}:{transaction_hash}:{log_index}:{claimed_address}:basenames"
            ),
            namespace: "basenames".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
            source_family: "basenames_base_primary".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(source_manifest_id),
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(42),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(transaction_hash.to_owned()),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": block_hash,
                "block_number": 42,
                "transaction_hash": transaction_hash,
                "transaction_index": 0,
                "log_index": log_index,
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            }),
            derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ReverseClaimed",
                "address": claimed_address,
                "coin_type": ENS_NATIVE_COIN_TYPE,
                "namespace": "basenames",
                "reverse_namespace": "basenames",
                "reverse_label": claimed_address.trim_start_matches("0x").to_ascii_lowercase(),
                "reverse_name": reverse_name,
                "reverse_node": reverse_node,
                "claim_provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
                    "contract_instance_id": Uuid::from_u128(0x45).to_string(),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        }
    }

    fn reverse_label_for_address(address: &str) -> String {
        address.trim_start_matches("0x").to_ascii_lowercase()
    }

    fn reverse_node_for_address(address: &str) -> String {
        let reverse_label = reverse_label_for_address(address);
        namehash_hex(&[
            reverse_label.into_bytes(),
            b"addr".to_vec(),
            b"reverse".to_vec(),
        ])
    }

    fn resolver_raw_log(
        emitting_address: &str,
        topics: Vec<String>,
        data: Vec<u8>,
        log_index: i64,
    ) -> AuthorityRawLogRow {
        AuthorityRawLogRow {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)
                .expect("test timestamp must be valid"),
            transaction_hash: "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_owned(),
            topics,
            data,
            canonicality_state: CanonicalityState::Canonical,
            source_manifest_id: 3,
            namespace: "ens".to_owned(),
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
            manifest_version: 1,
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        }
    }

    #[test]
    fn build_authority_observation_decodes_resolver_record_logs() -> Result<()> {
        let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
        let resolver_address = "0x00000000000000000000000000000000000000cc";

        let text_observation = build_authority_observation(&resolver_raw_log(
            resolver_address,
            vec![
                text_changed_topic0(),
                alice.namehash.clone(),
                keccak256_hex(b"com.twitter"),
            ],
            encode_dynamic_string_log_data("com.twitter"),
            0,
        ))?
        .context("TextChanged observation should decode")?;
        assert_eq!(
            text_observation,
            AuthorityObservation::RecordChanged(RecordChangeObservation {
                namehash: alice.namehash.clone(),
                resolver: resolver_address.to_owned(),
                selector: RecordSelector {
                    record_key: "text".to_owned(),
                    record_family: "text".to_owned(),
                    selector_key: None,
                },
                raw_name: None,
                reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 0)
                    .reference(),
            })
        );

        let name_observation = build_authority_observation(&resolver_raw_log(
            resolver_address,
            vec![name_changed_topic0(), alice.namehash.clone()],
            encode_dynamic_string_log_data("alice.eth"),
            1,
        ))?
        .context("NameChanged observation should decode")?;
        assert_eq!(
            name_observation,
            AuthorityObservation::RecordChanged(RecordChangeObservation {
                namehash: alice.namehash.clone(),
                resolver: resolver_address.to_owned(),
                selector: RecordSelector {
                    record_key: "name".to_owned(),
                    record_family: "name".to_owned(),
                    selector_key: None,
                },
                raw_name: Some("alice.eth".to_owned()),
                reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 1)
                    .reference(),
            })
        );

        let addr_observation = build_authority_observation(&resolver_raw_log(
            resolver_address,
            vec![addr_changed_topic0(), alice.namehash.clone()],
            encode_resolver_addr_changed_log_data("0x00000000000000000000000000000000000000aa"),
            2,
        ))?
        .context("AddrChanged observation should decode")?;
        assert_eq!(
            addr_observation,
            AuthorityObservation::RecordChanged(RecordChangeObservation {
                namehash: alice.namehash.clone(),
                resolver: resolver_address.to_owned(),
                selector: RecordSelector {
                    record_key: "addr:60".to_owned(),
                    record_family: "addr".to_owned(),
                    selector_key: Some("60".to_owned()),
                },
                raw_name: None,
                reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 2)
                    .reference(),
            })
        );

        let multicoin_addr_observation = build_authority_observation(&resolver_raw_log(
            resolver_address,
            vec![address_changed_topic0(), alice.namehash.clone()],
            encode_resolver_address_changed_log_data(61, &[0xde, 0xad, 0xbe, 0xef]),
            3,
        ))?
        .context("AddressChanged observation should decode")?;
        assert_eq!(
            multicoin_addr_observation,
            AuthorityObservation::RecordChanged(RecordChangeObservation {
                namehash: alice.namehash.clone(),
                resolver: resolver_address.to_owned(),
                selector: RecordSelector {
                    record_key: "addr:61".to_owned(),
                    record_family: "addr".to_owned(),
                    selector_key: Some("61".to_owned()),
                },
                raw_name: None,
                reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 3)
                    .reference(),
            })
        );

        let record_version_observation = build_authority_observation(&resolver_raw_log(
            resolver_address,
            vec![version_changed_topic0(), alice.namehash.clone()],
            encode_resolver_version_changed_log_data(7),
            4,
        ))?
        .context("VersionChanged observation should decode")?;
        assert_eq!(
            record_version_observation,
            AuthorityObservation::RecordVersionChanged(RecordVersionObservation {
                namehash: alice.namehash,
                resolver: resolver_address.to_owned(),
                record_version: 7,
                reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 4)
                    .reference(),
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_persists_registrar_identity_rows_idempotently()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_registrar_l1/v1.toml",
        )
        .await?;
        let contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            manifest_id,
            "contract",
            "registrar",
            contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            manifest_id,
        )
        .await?;
        upsert_raw_blocks(
            database.pool(),
            &[raw_block(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                42,
                1_700_000_042,
            )],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first.scanned_log_count, 1);
        assert_eq!(first.matched_log_count, 1);
        assert_eq!(first.total_name_surface_count, 1);
        assert_eq!(first.total_resource_count, 1);
        assert_eq!(first.total_surface_binding_count, 1);
        assert_eq!(first.total_normalized_event_count, 5);
        assert_eq!(
            first.by_kind.get(EVENT_KIND_REGISTRATION_GRANTED),
            Some(&1_usize)
        );
        assert_eq!(first.by_kind.get(EVENT_KIND_EXPIRY_CHANGED), Some(&1_usize));
        assert_eq!(
            first.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
            Some(&1_usize)
        );
        assert_eq!(first.by_kind.get(EVENT_KIND_SURFACE_BOUND), Some(&1_usize));
        assert_eq!(
            first.by_kind.get(EVENT_KIND_AUTHORITY_EPOCH_CHANGED),
            Some(&1_usize)
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second.scanned_log_count, 1);
        assert_eq!(second.matched_log_count, 1);
        assert_eq!(second.total_name_surface_count, 1);
        assert_eq!(second.total_resource_count, 1);
        assert_eq!(second.total_surface_binding_count, 1);
        assert_eq!(second.total_normalized_event_count, 5);

        assert!(
            load_name_surface(database.pool(), "ens:alice.eth")
                .await?
                .is_some()
        );
        let bindings =
            load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
        assert_eq!(bindings.len(), 1);
        assert_eq!(
            bindings[0].binding_kind,
            SurfaceBindingKind::DeclaredRegistryPath
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
                .fetch_one(database.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
                .fetch_one(database.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
                .fetch_one(database.pool())
                .await?,
            5
        );
        assert_eq!(
            load_normalized_event_counts_by_kind(database.pool(), "ens").await?,
            BTreeMap::from([
                (EVENT_KIND_AUTHORITY_EPOCH_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_EXPIRY_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_PERMISSION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_REGISTRATION_GRANTED.to_owned(), 1_usize),
                (EVENT_KIND_SURFACE_BOUND.to_owned(), 1_usize),
            ])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_emits_resolver_changed_idempotently() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let registrar_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_registrar_l1/v1.toml",
        )
        .await?;
        let registrar_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            registrar_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            registrar_manifest_id,
            "contract",
            "registrar",
            registrar_contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            registrar_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            registrar_manifest_id,
        )
        .await?;

        let registry_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_registry_l1/v1.toml",
        )
        .await?;
        let registry_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            registry_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            registry_manifest_id,
            "contract",
            "registry",
            registry_contract_instance_id,
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            registry_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000bb",
            registry_manifest_id,
        )
        .await?;

        let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let transaction_hash = "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_timestamp = 1_700_000_042;
        upsert_raw_blocks(
            database.pool(),
            &[raw_block(
                block_hash,
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                42,
                block_timestamp,
            )],
        )
        .await?;
        let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        name_registered_topic0(),
                        keccak256_hex(b"alice"),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                    ],
                    data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first.scanned_log_count, 2);
        assert_eq!(first.matched_log_count, 2);
        assert_eq!(first.total_name_surface_count, 1);
        assert_eq!(first.total_resource_count, 1);
        assert_eq!(first.total_surface_binding_count, 1);
        assert_eq!(first.total_normalized_event_count, 7);
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
            Some(&1_usize)
        );
        assert_eq!(
            first.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
            Some(&2_usize)
        );

        let expected_identity = format!(
            "{}:{}:resolver:{}:{}:{}",
            DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            EVENT_KIND_RESOLVER_CHANGED,
            block_hash,
            transaction_hash,
            1
        );
        let resolver_event_resource_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
        )
        .fetch_one(database.pool())
        .await?;
        let authority_resource_id =
            sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(resolver_event_resource_id, authority_resource_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(authority_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resource' LIMIT 1"
            )
            .fetch_one(database.pool())
            .await?,
            "resource".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resolver' LIMIT 1"
            )
            .fetch_one(database.pool())
            .await?,
            "resolver".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT event_identity FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            expected_identity
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "ens:alice.eth".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT before_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            None
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "0x00000000000000000000000000000000000000cc".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'namehash' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            alice.namehash.clone()
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second.scanned_log_count, 2);
        assert_eq!(second.matched_log_count, 2);
        assert_eq!(second.total_normalized_event_count, 7);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
                .fetch_one(database.pool())
                .await?,
            7
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_emits_reverse_claim_source_observations() -> Result<()>
    {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let reverse_manifest_id = insert_active_contract_fixture(
            database.pool(),
            "ens_v1_reverse_l1",
            "reverse_registrar",
            "0x00000000000000000000000000000000000000ad",
            Some(CONTRACT_ROLE_REVERSE_REGISTRAR),
            "manifests/ens/ens_v1_reverse_l1/v1.toml",
        )
        .await?;
        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "registry",
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            "manifests/ens/ens_v1_registry_l1/v1.toml",
        )
        .await?;
        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "resolver",
            "0x00000000000000000000000000000000000000cc",
            Some("resolver"),
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;

        let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let claimed_address = "0x0000000000000000000000000000000000001234";
        let reverse_node = reverse_node_for_address(claimed_address);
        let reverse_name = format!(
            "{}.addr.reverse",
            reverse_label_for_address(claimed_address)
        );

        upsert_raw_blocks(
            database.pool(),
            &[raw_block(
                block_hash,
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                42,
                1_700_000_042,
            )],
        )
        .await?;
        upsert_normalized_events(
            database.pool(),
            &[reverse_claim_event(
                reverse_manifest_id,
                block_hash,
                transaction_hash,
                0,
                claimed_address,
                &reverse_node,
                &reverse_name,
            )],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), reverse_node.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 2,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![name_changed_topic0(), reverse_node.clone()],
                    data: encode_dynamic_string_log_data("alice.eth"),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 3,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![version_changed_topic0(), reverse_node.clone()],
                    data: encode_resolver_version_changed_log_data(7),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first.scanned_log_count, 3);
        assert_eq!(first.matched_log_count, 3);
        assert_eq!(first.total_name_surface_count, 0);
        assert_eq!(first.total_resource_count, 0);
        assert_eq!(first.total_surface_binding_count, 0);
        assert_eq!(first.total_normalized_event_count, 3);
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
            Some(&1_usize)
        );
        assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&1_usize));
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
            Some(&1_usize)
        );

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged' AND logical_name_id IS NULL AND resource_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE event_kind = 'ResolverChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            claimed_address.to_ascii_lowercase()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "alice.eth".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            reverse_node.to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'RecordVersionChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            CONTRACT_ROLE_REVERSE_REGISTRAR.to_owned()
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second.scanned_log_count, 3);
        assert_eq!(second.matched_log_count, 3);
        assert_eq!(second.total_normalized_event_count, 3);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE logical_name_id IS NULL AND event_kind IN ('ResolverChanged', 'RecordChanged', 'RecordVersionChanged')"
            )
            .fetch_one(database.pool())
            .await?,
            3
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_emits_supported_record_change_events_idempotently()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            "registrar",
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            "manifests/ens/ens_v1_registrar_l1/v1.toml",
        )
        .await?;
        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "registry",
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            "manifests/ens/ens_v1_registry_l1/v1.toml",
        )
        .await?;
        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "resolver",
            "0x00000000000000000000000000000000000000cc",
            Some("resolver"),
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;

        let block_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let transaction_hash = "0xtxcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        upsert_raw_blocks(
            database.pool(),
            &[raw_block(
                block_hash,
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                42,
                1_700_000_042,
            )],
        )
        .await?;

        let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        name_registered_topic0(),
                        keccak256_hex(b"alice"),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                    ],
                    data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 2,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![
                        text_changed_topic0(),
                        alice.namehash.clone(),
                        keccak256_hex(b"com.twitter"),
                    ],
                    data: encode_dynamic_string_log_data("com.twitter"),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 3,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![addr_changed_topic0(), alice.namehash.clone()],
                    data: encode_resolver_addr_changed_log_data(
                        "0x00000000000000000000000000000000000000aa",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 4,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![version_changed_topic0(), alice.namehash.clone()],
                    data: encode_resolver_version_changed_log_data(7),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first.scanned_log_count, 5);
        assert_eq!(first.matched_log_count, 5);
        assert_eq!(first.total_resource_count, 1);
        assert_eq!(first.total_normalized_event_count, 10);
        assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&2_usize));
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
            Some(&1_usize)
        );

        let authority_resource_id =
            sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
                .fetch_one(database.pool())
                .await?;
        let record_change_resource_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordChanged' ORDER BY log_index",
        )
        .fetch_all(database.pool())
        .await?;
        assert_eq!(record_change_resource_ids, vec![authority_resource_id; 2]);
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordVersionChanged'",
            )
            .fetch_one(database.pool())
            .await?,
            authority_resource_id
        );
        assert_eq!(
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT ARRAY_AGG(after_state->>'record_key' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            vec!["text".to_owned(), "addr:60".to_owned()]
        );
        assert_eq!(
            sqlx::query_scalar::<_, Vec<Option<String>>>(
                "SELECT ARRAY_AGG(after_state->>'selector_key' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            vec![None, Some("60".to_owned())]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'record_version' FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "7".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged')"
            )
            .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
            .fetch_one(database.pool())
            .await?,
            3
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second.scanned_log_count, 5);
        assert_eq!(second.matched_log_count, 5);
        assert_eq!(second.total_normalized_event_count, 10);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_emits_basenames_base_authority_events_idempotently()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
            "base-mainnet",
            "basenames_v1",
            "registrar",
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            "manifests/basenames/basenames_base_registrar/v1.toml",
        )
        .await?;
        insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "base-mainnet",
            "basenames_v1",
            "registry",
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            "manifests/basenames/basenames_base_registry/v1.toml",
        )
        .await?;
        insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            "base-mainnet",
            "basenames_v1",
            "resolver",
            "0x00000000000000000000000000000000000000cc",
            Some("resolver"),
            "manifests/basenames/basenames_base_resolver/v1.toml",
        )
        .await?;

        let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        upsert_raw_blocks(
            database.pool(),
            &[raw_block_on_chain(
                "base-mainnet",
                block_hash,
                Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                42,
                1_700_000_042,
            )],
        )
        .await?;

        let alice = observe_registrar_name_with_version(
            "alice",
            AuthorityProfile::Basenames,
            ENS_NORMALIZER_VERSION,
        )?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        name_registered_topic0(),
                        keccak256_hex(b"alice"),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                    ],
                    data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 2,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![
                        text_changed_topic0(),
                        alice.namehash.clone(),
                        keccak256_hex(b"com.twitter"),
                    ],
                    data: encode_dynamic_string_log_data("com.twitter"),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 3,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![version_changed_topic0(), alice.namehash.clone()],
                    data: encode_resolver_version_changed_log_data(7),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
        assert_eq!(first.scanned_log_count, 4);
        assert_eq!(first.matched_log_count, 4);
        assert_eq!(first.total_name_surface_count, 1);
        assert_eq!(first.total_resource_count, 1);
        assert_eq!(first.total_surface_binding_count, 1);
        assert_eq!(first.total_normalized_event_count, 9);
        assert_eq!(
            first.by_kind,
            BTreeMap::from([
                (EVENT_KIND_AUTHORITY_EPOCH_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_EXPIRY_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_PERMISSION_CHANGED.to_owned(), 2_usize),
                (EVENT_KIND_RECORD_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_REGISTRATION_GRANTED.to_owned(), 1_usize),
                (EVENT_KIND_RESOLVER_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_SURFACE_BOUND.to_owned(), 1_usize),
            ])
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
        assert_eq!(second.scanned_log_count, 4);
        assert_eq!(second.matched_log_count, 4);
        assert_eq!(second.total_normalized_event_count, 9);

        let logical_name_id = "basenames:alice.base.eth";
        let surface = load_name_surface(database.pool(), logical_name_id)
            .await?
            .context("Basenames name surface should persist")?;
        assert_eq!(surface.namespace, "basenames");
        assert_eq!(surface.canonical_display_name, "alice.base.eth");
        assert_eq!(surface.namehash, alice.namehash);
        assert_eq!(surface.labelhashes, alice.labelhashes);

        assert_eq!(
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT ARRAY_AGG(DISTINCT namespace ORDER BY namespace) FROM normalized_events"
            )
            .fetch_one(database.pool())
            .await?,
            vec!["basenames".to_owned()]
        );
        assert_eq!(
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT ARRAY_AGG(event_kind ORDER BY log_index) FROM normalized_events WHERE source_family = $1"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
            .fetch_one(database.pool())
            .await?,
            vec![
                EVENT_KIND_RECORD_CHANGED.to_owned(),
                EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            logical_name_id.to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'namehash' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            alice.namehash
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind = 'PermissionChanged'"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR)
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind = 'PermissionChanged'"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
            .fetch_one(database.pool())
            .await?,
            1
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_backfills_basenames_primary_claim_source_observations()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let reverse_manifest_id = insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            "basenames_base_primary",
            "base-mainnet",
            "basenames_v1",
            "reverse_registrar",
            "0x00000000000000000000000000000000000000ad",
            Some(CONTRACT_ROLE_REVERSE_REGISTRAR),
            "manifests/basenames/basenames_base_primary/v1.toml",
        )
        .await?;
        insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            "base-mainnet",
            "basenames_v1",
            "registry",
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            "manifests/basenames/basenames_base_registry/v1.toml",
        )
        .await?;
        insert_active_contract_fixture_with_manifest(
            database.pool(),
            "basenames",
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            "base-mainnet",
            "basenames_v1",
            "resolver",
            "0x00000000000000000000000000000000000000cc",
            Some("resolver"),
            "manifests/basenames/basenames_base_resolver/v1.toml",
        )
        .await?;

        let block_hash = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let transaction_hash = "0xtxeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let claimed_address = "0x0000000000000000000000000000000000005678";
        let reverse_node = reverse_node_for_address(claimed_address);
        let reverse_name = format!(
            "{}.addr.reverse",
            reverse_label_for_address(claimed_address)
        );

        upsert_raw_blocks(
            database.pool(),
            &[raw_block_on_chain(
                "base-mainnet",
                block_hash,
                Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
                42,
                1_700_000_042,
            )],
        )
        .await?;
        upsert_normalized_events(
            database.pool(),
            &[basenames_reverse_claim_event(
                reverse_manifest_id,
                block_hash,
                transaction_hash,
                0,
                claimed_address,
                &reverse_node,
                &reverse_name,
            )],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), reverse_node.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 2,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![name_changed_topic0(), reverse_node.clone()],
                    data: encode_dynamic_string_log_data("alice.base.eth"),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 3,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![version_changed_topic0(), reverse_node.clone()],
                    data: encode_resolver_version_changed_log_data(7),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let first = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
        assert_eq!(first.scanned_log_count, 3);
        assert_eq!(first.matched_log_count, 3);
        assert_eq!(first.total_name_surface_count, 0);
        assert_eq!(first.total_resource_count, 0);
        assert_eq!(first.total_surface_binding_count, 0);
        assert_eq!(first.total_normalized_event_count, 3);
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
            Some(&1_usize)
        );
        assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&1_usize));
        assert_eq!(
            first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
            Some(&1_usize)
        );

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'ResolverChanged' AND logical_name_id IS NULL AND resource_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'ResolverChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            claimed_address.to_ascii_lowercase()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'raw_name' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "alice.base.eth".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            reverse_node.to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'source_family' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordVersionChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "basenames_base_primary".to_owned()
        );

        let second = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
        assert_eq!(second.scanned_log_count, 3);
        assert_eq!(second.matched_log_count, 3);
        assert_eq!(second.total_normalized_event_count, 3);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind IN ('ResolverChanged', 'RecordChanged', 'RecordVersionChanged')"
            )
            .fetch_one(database.pool())
            .await?,
            3
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_drops_resolver_record_logs_without_current_context()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            "registrar",
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            "manifests/ens/ens_v1_registrar_l1/v1.toml",
        )
        .await?;
        insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "registry",
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            "manifests/ens/ens_v1_registry_l1/v1.toml",
        )
        .await?;
        let resolver_manifest_id = insert_active_contract_fixture(
            database.pool(),
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "resolver",
            "0x00000000000000000000000000000000000000cc",
            Some("resolver"),
            "manifests/ens/ens_v1_resolver_l1/v1.toml",
        )
        .await?;
        let alternate_resolver_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            alternate_resolver_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            resolver_manifest_id,
            "contract",
            "resolver_alt",
            alternate_resolver_contract_instance_id,
            "0x00000000000000000000000000000000000000dd",
            Some("resolver"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            alternate_resolver_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000dd",
            resolver_manifest_id,
        )
        .await?;

        let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        upsert_raw_blocks(
            database.pool(),
            &[raw_block(
                block_hash,
                Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
                42,
                1_700_000_042,
            )],
        )
        .await?;

        let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        name_registered_topic0(),
                        keccak256_hex(b"alice"),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                    ],
                    data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                    topics: vec![
                        text_changed_topic0(),
                        alice.namehash.clone(),
                        keccak256_hex(b"com.twitter"),
                    ],
                    data: encode_dynamic_string_log_data("com.twitter"),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 2,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash: block_hash.to_owned(),
                    block_number: 42,
                    transaction_hash: transaction_hash.to_owned(),
                    transaction_index: 0,
                    log_index: 3,
                    emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                    topics: vec![
                        text_changed_topic0(),
                        alice.namehash.clone(),
                        keccak256_hex(b"com.github"),
                    ],
                    data: encode_dynamic_string_log_data("com.github"),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.scanned_log_count, 4);
        assert_eq!(summary.matched_log_count, 4);
        assert_eq!(summary.total_normalized_event_count, 7);
        assert_eq!(summary.by_kind.get(EVENT_KIND_RECORD_CHANGED), None);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_unwrapped_authority_partitions_permission_events_by_authoritative_resource_id()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let registrar_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_registrar_l1/v1.toml",
        )
        .await?;
        let registrar_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            registrar_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            registrar_manifest_id,
            "contract",
            "registrar",
            registrar_contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("registrar"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            registrar_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            registrar_manifest_id,
        )
        .await?;

        let registry_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_registry_l1/v1.toml",
        )
        .await?;
        let registry_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            registry_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            registry_manifest_id,
            "contract",
            "registry",
            registry_contract_instance_id,
            "0x00000000000000000000000000000000000000bb",
            Some("registry"),
            Some("none"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            registry_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000bb",
            registry_manifest_id,
        )
        .await?;

        let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
        let registration_expiry = 1_700_000_100;
        upsert_raw_blocks(
            database.pool(),
            &[
                raw_block(
                    "0x1111111111111111111111111111111111111111111111111111111111111111",
                    Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                    41,
                    1_700_000_010,
                ),
                raw_block(
                    "0x2222222222222222222222222222222222222222222222222222222222222222",
                    Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
                    42,
                    1_700_000_042,
                ),
                raw_block(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    Some("0x2222222222222222222222222222222222222222222222222222222222222222"),
                    43,
                    1_700_000_050,
                ),
                raw_block(
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                    Some("0x3333333333333333333333333333333333333333333333333333333333333333"),
                    44,
                    release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
                        .unix_timestamp(),
                ),
            ],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash:
                        "0x1111111111111111111111111111111111111111111111111111111111111111"
                            .to_owned(),
                    block_number: 41,
                    transaction_hash:
                        "0xtx11111111111111111111111111111111111111111111111111111111111111"
                            .to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_owner_topic0(), eth_node(), keccak256_hex(b"alice")],
                    data: abi_word_address("0x0000000000000000000000000000000000000003").to_vec(),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash:
                        "0x2222222222222222222222222222222222222222222222222222222222222222"
                            .to_owned(),
                    block_number: 42,
                    transaction_hash:
                        "0xtx22222222222222222222222222222222222222222222222222222222222222"
                            .to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        name_registered_topic0(),
                        keccak256_hex(b"alice"),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                    ],
                    data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash:
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                            .to_owned(),
                    block_number: 43,
                    transaction_hash:
                        "0xtx33333333333333333333333333333333333333333333333333333333333333"
                            .to_owned(),
                    transaction_index: 0,
                    log_index: 0,
                    emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                    topics: vec![
                        transfer_topic0(),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000001",
                        )),
                        hex_string(&abi_word_address(
                            "0x0000000000000000000000000000000000000002",
                        )),
                        alice.labelhashes[0].clone(),
                    ],
                    data: Vec::new(),
                    canonicality_state: CanonicalityState::Canonical,
                },
                RawLog {
                    chain_id: "ethereum-mainnet".to_owned(),
                    block_hash:
                        "0x3333333333333333333333333333333333333333333333333333333333333333"
                            .to_owned(),
                    block_number: 43,
                    transaction_hash:
                        "0xtx33333333333333333333333333333333333333333333333333333333333333"
                            .to_owned(),
                    transaction_index: 0,
                    log_index: 1,
                    emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                    topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                    data: encode_registry_new_resolver_log_data(
                        "0x00000000000000000000000000000000000000cc",
                    ),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;

        let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.total_resource_count, 2);
        assert_eq!(
            summary.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
            Some(&6_usize)
        );

        let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'registrar' LIMIT 1",
        )
        .fetch_one(database.pool())
        .await?;
        let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'registry_only' LIMIT 1",
        )
        .fetch_one(database.pool())
        .await?;
        assert_ne!(registrar_resource_id, registry_resource_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(registrar_resource_id)
            .fetch_one(database.pool())
            .await?,
            4
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND block_number = 44"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'subject' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND after_state->'scope'->>'kind' = 'resource' AND after_state->>'subject' <> '' ORDER BY block_number DESC LIMIT 1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            "0x0000000000000000000000000000000000000003".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'resolver_address' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND after_state->'scope'->>'kind' = 'resolver' ORDER BY block_number DESC LIMIT 1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            "0x00000000000000000000000000000000000000cc".to_owned()
        );

        database.cleanup().await
    }
}
