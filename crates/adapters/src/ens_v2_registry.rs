use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryReconciliationSummary, WatchedContractSource,
    load_watched_contracts, reconcile_discovery_observations,
};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, load_name_surface_including_noncanonical, load_resource_including_noncanonical,
    load_surface_binding_including_noncanonical, load_token_lineage_including_noncanonical,
    upsert_name_surfaces, upsert_normalized_events, upsert_resources, upsert_surface_bindings,
    upsert_token_lineages,
};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time::OffsetDateTime},
};

const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE: &str = "ens_v2_registry_resource_surface";
const RESOLVER_EDGE_KIND: &str = "resolver";
const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
const EVENT_KIND_REGISTRATION_RESERVED: &str = "RegistrationReserved";
const EVENT_KIND_REGISTRATION_RELEASED: &str = "RegistrationReleased";
const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
const EVENT_KIND_PARENT_CHANGED: &str = "ParentChanged";
const EVENT_KIND_TOKEN_RESOURCE_LINKED: &str = "TokenResourceLinked";
const EVENT_KIND_TOKEN_REGENERATED: &str = "TokenRegenerated";
const EVENT_KIND_SURFACE_BOUND: &str = "SurfaceBound";

const LABEL_REGISTERED_SIGNATURE: &str =
    "LabelRegistered(uint256,bytes32,string,address,uint64,address)";
const LABEL_RESERVED_SIGNATURE: &str = "LabelReserved(uint256,bytes32,string,uint64,address)";
const LABEL_UNREGISTERED_SIGNATURE: &str = "LabelUnregistered(uint256,address)";
const EXPIRY_UPDATED_SIGNATURE: &str = "ExpiryUpdated(uint256,uint64,address)";
const SUBREGISTRY_UPDATED_SIGNATURE: &str = "SubregistryUpdated(uint256,address,address)";
const RESOLVER_UPDATED_SIGNATURE: &str = "ResolverUpdated(uint256,address,address)";
const TOKEN_REGENERATED_SIGNATURE: &str = "TokenRegenerated(uint256,uint256)";
const PARENT_UPDATED_SIGNATURE: &str = "ParentUpdated(address,string,address)";
const TOKEN_RESOURCE_SIGNATURE: &str = "TokenResource(uint256,uint256)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistryResourceSurfaceSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_name_surface_count: usize,
    pub total_resource_count: usize,
    pub total_surface_binding_count: usize,
    pub total_normalized_event_count: usize,
    pub active_discovery_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    pub by_kind: BTreeMap<String, usize>,
}

impl EnsV2RegistryResourceSurfaceSyncSummary {
    pub fn empty(scanned_log_count: usize) -> Self {
        Self {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            active_discovery_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            by_kind: BTreeMap::new(),
        }
    }
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
    role: Option<String>,
    source: WatchedContractSource,
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
    role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryRawLogRow {
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
    emitting_contract_instance_id: Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryNameState {
    token_id: String,
    labelhash: String,
    label: String,
    full_name: String,
    name: NameMetadata,
    owner: Option<String>,
    expiry: Option<i64>,
    status: &'static str,
    first_ref: ObservationRef,
    current_ref: ObservationRef,
    registry_address: String,
    registry_contract_instance_id: Uuid,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
    resource: Option<RegistryResourceLink>,
    resolver: Option<String>,
    subregistry: Option<String>,
    binding_kind: SurfaceBindingKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryResourceLink {
    upstream_resource: String,
    observed_token_id: String,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    linked_ref: ObservationRef,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservationRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: Uuid,
    canonicality_state: CanonicalityState,
    namespace: String,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RegistryObservation {
    LabelRegistered {
        token_id: String,
        labelhash: String,
        label: String,
        owner: String,
        expiry: i64,
        sender: String,
        reference: ObservationRef,
    },
    LabelReserved {
        token_id: String,
        labelhash: String,
        label: String,
        expiry: i64,
        sender: String,
        reference: ObservationRef,
    },
    LabelUnregistered {
        token_id: String,
        sender: String,
        reference: ObservationRef,
    },
    ExpiryUpdated {
        token_id: String,
        new_expiry: i64,
        sender: String,
        reference: ObservationRef,
    },
    SubregistryUpdated {
        token_id: String,
        subregistry: String,
        sender: String,
        reference: ObservationRef,
    },
    ResolverUpdated {
        token_id: String,
        resolver: String,
        sender: String,
        reference: ObservationRef,
    },
    TokenResource {
        token_id: String,
        upstream_resource: String,
        reference: ObservationRef,
    },
    TokenRegenerated {
        old_token_id: String,
        new_token_id: String,
        reference: ObservationRef,
    },
    ParentUpdated {
        parent: String,
        label: String,
        sender: String,
        reference: ObservationRef,
    },
}

pub async fn sync_ens_v2_registry_resource_surface(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
    }

    let raw_logs = load_registry_raw_logs(pool, chain, &active_emitters).await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(
            scanned_log_count,
        ));
    }

    let mut matched_log_count = 0usize;
    let mut registry_suffix_by_address = initial_registry_suffixes(&active_emitters);
    let mut registry_contract_by_address = active_emitters
        .iter()
        .map(|emitter| (emitter.address.clone(), emitter.contract_instance_id))
        .collect::<HashMap<_, _>>();
    let mut states_by_registry_token = BTreeMap::<(String, String), RegistryNameState>::new();
    let mut linked_resource_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut closed_bindings = BTreeMap::<Uuid, SurfaceBinding>::new();
    let mut token_aliases = HashMap::<(String, String), (String, String)>::new();
    let mut observations = Vec::<DiscoveryObservation>::new();
    let mut graph_events = Vec::<NormalizedEvent>::new();

    for raw_log in &raw_logs {
        let Some(observation) = build_registry_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;
        apply_registry_observation(
            observation,
            &mut registry_suffix_by_address,
            &mut registry_contract_by_address,
            &mut states_by_registry_token,
            &mut linked_resource_states,
            &mut closed_bindings,
            &mut token_aliases,
            &mut observations,
            &mut graph_events,
        )?;
    }

    let latest_observations = latest_discovery_observations(observations)?;
    let reconciliation = reconcile_discovery_observations_by_source(pool, &latest_observations)
        .await
        .with_context(|| format!("failed to reconcile ENSv2 discovery observations for {chain}"))?;

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = graph_events;

    for state in linked_resource_states.values() {
        let Some(link) = state.resource.as_ref() else {
            continue;
        };
        token_lineages.push(build_token_lineage(pool, state, link).await?);
        resources.push(build_resource(pool, state, link).await?);
        surfaces.push(build_name_surface(pool, &state.name, &state.first_ref).await?);
        if let Some(closed_binding) = closed_bindings.get(&link.surface_binding_id) {
            bindings.push(closed_binding.clone());
        } else {
            bindings.push(build_surface_binding(pool, state, link).await?);
        }
        events.extend(build_resource_events(state, link));
    }

    let by_kind = count_events_by_kind(&events);
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings_close_before_open(pool, &bindings).await?;
    upsert_normalized_events(pool, &events).await?;

    Ok(EnsV2RegistryResourceSurfaceSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        active_discovery_observation_count: latest_observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: reconciliation.active_edge_count,
        admitted_edge_count: reconciliation.admitted_edge_count,
        inserted_edge_count: reconciliation.inserted_edge_count,
        deactivated_edge_count: reconciliation.deactivated_edge_count,
        by_kind,
    })
}

async fn upsert_surface_bindings_close_before_open(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<()> {
    let closed_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_some())
        .cloned()
        .collect::<Vec<_>>();
    let open_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_none())
        .cloned()
        .collect::<Vec<_>>();

    upsert_surface_bindings(pool, &closed_bindings).await?;
    upsert_surface_bindings(pool, &open_bindings).await?;
    Ok(())
}

fn apply_registry_observation(
    observation: RegistryObservation,
    registry_suffix_by_address: &mut HashMap<String, String>,
    registry_contract_by_address: &mut HashMap<String, Uuid>,
    states_by_registry_token: &mut BTreeMap<(String, String), RegistryNameState>,
    linked_resource_states: &mut BTreeMap<Uuid, RegistryNameState>,
    closed_bindings: &mut BTreeMap<Uuid, SurfaceBinding>,
    token_aliases: &mut HashMap<(String, String), (String, String)>,
    observations: &mut Vec<DiscoveryObservation>,
    graph_events: &mut Vec<NormalizedEvent>,
) -> Result<()> {
    match observation {
        RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let name = observe_name(&reference.namespace, &full_name, &reference, &label)?;
            let state = RegistryNameState {
                token_id,
                labelhash,
                label,
                full_name,
                name,
                owner: Some(owner),
                expiry: Some(expiry),
                status: "registered",
                first_ref: reference.clone(),
                current_ref: reference.clone(),
                registry_address: reference.emitting_address.clone(),
                registry_contract_instance_id: reference.emitting_contract_instance_id,
                source_manifest_id: reference.source_manifest_id,
                source_family: reference.source_family.clone(),
                manifest_version: reference.manifest_version,
                resource: None,
                resolver: None,
                subregistry: None,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            };
            graph_events.push(normalized_event(
                &reference,
                Some(state.name.logical_name_id.clone()),
                None,
                EVENT_KIND_REGISTRATION_GRANTED,
                json!({}),
                json!({
                    "source_event": "LabelRegistered",
                    "status": "registered",
                    "token_id": state.token_id,
                    "label": state.label,
                    "labelhash": state.labelhash,
                    "registrant": state.owner,
                    "expiry": expiry,
                    "sender": sender,
                    "resource_pending": true,
                }),
                format!("label-registered:{}", state.token_id),
            ));
            states_by_registry_token.insert(key, state);
        }
        RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let name = observe_name(&reference.namespace, &full_name, &reference, &label)?;
            states_by_registry_token.insert(
                key,
                RegistryNameState {
                    token_id: token_id.clone(),
                    labelhash: labelhash.clone(),
                    label,
                    full_name,
                    name,
                    owner: None,
                    expiry: Some(expiry),
                    status: "reserved",
                    first_ref: reference.clone(),
                    current_ref: reference.clone(),
                    registry_address: reference.emitting_address.clone(),
                    registry_contract_instance_id: reference.emitting_contract_instance_id,
                    source_manifest_id: reference.source_manifest_id,
                    source_family: reference.source_family.clone(),
                    manifest_version: reference.manifest_version,
                    resource: None,
                    resolver: None,
                    subregistry: None,
                    binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                },
            );
            graph_events.push(normalized_event(
                &reference,
                None,
                None,
                EVENT_KIND_REGISTRATION_RESERVED,
                json!({}),
                json!({
                    "source_event": "LabelReserved",
                    "status": "reserved",
                    "token_id": token_id,
                    "labelhash": labelhash,
                    "expiry": expiry,
                    "sender": sender,
                }),
                format!("label-reserved:{token_id}"),
            ));
        }
        RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                states_by_registry_token,
                token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                if let Some(binding) = closed_surface_binding_for_unregister(state, &reference) {
                    closed_bindings.insert(binding.surface_binding_id, binding);
                }
                state.status = "unregistered";
                state.current_ref = reference.clone();
                graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_REGISTRATION_RELEASED,
                    json!({"status": "registered"}),
                    json!({
                        "source_event": "LabelUnregistered",
                        "status": "unregistered",
                        "token_id": token_id,
                        "sender": sender,
                    }),
                    format!("label-unregistered:{token_id}"),
                ));
            }
        }
        RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                states_by_registry_token,
                token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before_expiry = state.expiry;
                state.expiry = Some(new_expiry);
                state.current_ref = reference.clone();
                remember_linked_resource_state(linked_resource_states, state);
                graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_EXPIRY_CHANGED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "sender": sender,
                    }),
                    format!("expiry-updated:{token_id}"),
                ));
                graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_REGISTRATION_RENEWED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "labelhash": state.labelhash,
                    }),
                    format!("registration-renewed:{token_id}"),
                ));
            }
        }
        RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        } => {
            let mut logical_name_id = None;
            let mut resource_id = None;
            let mut observation_key = format!("{}:{token_id}", reference.emitting_address);
            if let Some(state) = state_for_token_mut(
                states_by_registry_token,
                token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.subregistry.clone();
                if before.as_deref() != Some(subregistry.as_str()) {
                    deactivate_registry_suffix(
                        registry_suffix_by_address,
                        before.as_deref(),
                        &state.full_name,
                    );
                }
                state.subregistry = Some(subregistry.clone());
                state.current_ref = reference.clone();
                logical_name_id = Some(state.name.logical_name_id.clone());
                resource_id = state.resource.as_ref().map(|link| link.resource_id);
                observation_key = format!("{}:{}", reference.emitting_address, state.name.namehash);
                if subregistry != ZERO_ADDRESS {
                    registry_suffix_by_address.insert(subregistry.clone(), state.full_name.clone());
                }
                remember_linked_resource_state(linked_resource_states, state);
                graph_events.push(normalized_event(
                    &reference,
                    logical_name_id.clone(),
                    resource_id,
                    EVENT_KIND_SUBREGISTRY_CHANGED,
                    json!({"subregistry": before}),
                    json!({
                        "source_event": "SubregistryUpdated",
                        "token_id": token_id,
                        "subregistry": null_if_zero_address(&subregistry),
                        "sender": sender,
                        "from_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "to_contract_instance_id": registry_contract_by_address
                            .get(&subregistry)
                            .map(ToString::to_string),
                    }),
                    format!("subregistry-updated:{token_id}"),
                ));
            }
            observations.push(DiscoveryObservation {
                chain: reference.chain_id.clone(),
                from_address: reference.emitting_address.clone(),
                to_address: subregistry.clone(),
                edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
                discovery_source: ens_v2_subregistry_discovery_source(&reference.chain_id),
                active_from_block_number: Some(reference.block_number),
                active_from_block_hash: Some(reference.block_hash.clone()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: json!({
                    "source": "raw_log",
                    "source_event": "SubregistryUpdated",
                    "observation_key": observation_key,
                    "token_id": token_id,
                    "from_address": reference.emitting_address,
                    "to_address": subregistry,
                    "logical_name_id": logical_name_id,
                    "resource_id": resource_id.map(|value| value.to_string()),
                    "chain_id": reference.chain_id,
                    "block_hash": reference.block_hash,
                    "block_number": reference.block_number,
                    "transaction_hash": reference.transaction_hash,
                    "transaction_index": reference.transaction_index,
                    "log_index": reference.log_index,
                    "tombstone": normalize_address(&subregistry) == ZERO_ADDRESS,
                }),
            });
        }
        RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                states_by_registry_token,
                token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.resolver.clone();
                state.resolver = Some(resolver.clone());
                state.current_ref = reference.clone();
                remember_linked_resource_state(linked_resource_states, state);
                graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_RESOLVER_CHANGED,
                    json!({"resolver": before}),
                    json!({
                        "source_event": "ResolverUpdated",
                        "token_id": token_id,
                        "resolver": null_if_zero_address(&resolver),
                        "sender": sender,
                    }),
                    format!("resolver-updated:{token_id}"),
                ));
                observations.push(DiscoveryObservation {
                    chain: reference.chain_id.clone(),
                    from_address: reference.emitting_address.clone(),
                    to_address: resolver.clone(),
                    edge_kind: RESOLVER_EDGE_KIND.to_owned(),
                    discovery_source: ens_v2_resolver_discovery_source(&reference.chain_id),
                    active_from_block_number: Some(reference.block_number),
                    active_from_block_hash: Some(reference.block_hash.clone()),
                    active_to_block_number: None,
                    active_to_block_hash: None,
                    provenance: json!({
                        "source": "raw_log",
                        "source_event": "ResolverUpdated",
                        "observation_key": format!("resolver:{}:{}", reference.emitting_address, state.name.namehash),
                        "token_id": token_id,
                        "from_address": reference.emitting_address,
                        "to_address": resolver.clone(),
                        "logical_name_id": state.name.logical_name_id,
                        "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                        "chain_id": reference.chain_id,
                        "block_hash": reference.block_hash,
                        "block_number": reference.block_number,
                        "transaction_hash": reference.transaction_hash,
                        "transaction_index": reference.transaction_index,
                        "log_index": reference.log_index,
                        "tombstone": normalize_address(&resolver) == ZERO_ADDRESS,
                    }),
                });
            }
        }
        RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                states_by_registry_token,
                token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let resource_id = deterministic_uuid(&format!(
                    "ens-v2-resource:{}:{}:{}",
                    reference.chain_id, reference.emitting_contract_instance_id, upstream_resource
                ));
                let token_lineage_id = deterministic_uuid(&format!(
                    "ens-v2-token-lineage:{}:{}:{}",
                    reference.chain_id, reference.emitting_contract_instance_id, upstream_resource
                ));
                let surface_binding_id = deterministic_uuid(&format!(
                    "ens-v2-surface-binding:{}:{}:{}:{}",
                    reference.chain_id,
                    reference.emitting_contract_instance_id,
                    upstream_resource,
                    state.name.logical_name_id
                ));
                state.resource = Some(RegistryResourceLink {
                    upstream_resource,
                    observed_token_id: token_id.clone(),
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                    linked_ref: reference.clone(),
                });
                state.current_ref = reference;
                remember_linked_resource_state(linked_resource_states, state);
            }
        }
        RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        } => {
            let canonical_key =
                resolve_token_key(token_aliases, &reference.emitting_address, &old_token_id)
                    .unwrap_or_else(|| (reference.emitting_address.clone(), old_token_id.clone()));
            if let Some(state) = states_by_registry_token.get_mut(&canonical_key) {
                let previous_token_id = state.token_id.clone();
                state.token_id = new_token_id.clone();
                state.current_ref = reference.clone();
                remember_linked_resource_state(linked_resource_states, state);
                token_aliases.insert(
                    (reference.emitting_address.clone(), old_token_id.clone()),
                    canonical_key.clone(),
                );
                token_aliases.insert(
                    (reference.emitting_address.clone(), new_token_id.clone()),
                    canonical_key,
                );
                graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_TOKEN_REGENERATED,
                    json!({"token_id": previous_token_id}),
                    json!({
                        "source_event": "TokenRegenerated",
                        "old_token_id": old_token_id,
                        "new_token_id": new_token_id,
                        "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                    }),
                    format!("token-regenerated:{old_token_id}:{new_token_id}"),
                ));
            }
        }
        RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
        } => {
            if let Some(full_name) =
                name_under_registry(&parent, &label, registry_suffix_by_address)
            {
                registry_suffix_by_address
                    .insert(reference.emitting_address.clone(), full_name.clone());
                graph_events.push(normalized_event(
                    &reference,
                    None,
                    None,
                    EVENT_KIND_PARENT_CHANGED,
                    json!({}),
                    json!({
                        "source_event": "ParentUpdated",
                        "parent": null_if_zero_address(&parent),
                        "label": label,
                        "registry_name": full_name,
                        "sender": sender,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "parent_contract_instance_id": registry_contract_by_address
                            .get(&parent)
                            .map(ToString::to_string),
                    }),
                    format!("parent-updated:{}", reference.emitting_address),
                ));
            }
        }
    }

    Ok(())
}

fn build_resource_events(
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    events.push(normalized_event(
        &link.linked_ref,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_TOKEN_RESOURCE_LINKED,
        json!({}),
        json!({
            "source_event": "TokenResource",
            "token_id": link.observed_token_id,
            "upstream_resource": link.upstream_resource,
            "resource_id": link.resource_id.to_string(),
            "token_lineage_id": link.token_lineage_id.to_string(),
            "current_token_id": state.token_id,
            "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        }),
        format!("token-resource-linked:{}", link.upstream_resource),
    ));
    events.push(normalized_event(
        &link.linked_ref,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_SURFACE_BOUND,
        json!({}),
        json!({
            "binding_kind": state.binding_kind.as_str(),
            "surface_binding_id": link.surface_binding_id.to_string(),
            "logical_name_id": state.name.logical_name_id,
            "resource_id": link.resource_id.to_string(),
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        format!("surface-bound:{}", link.surface_binding_id),
    ));
    if state.status == "registered" {
        events.push(normalized_event(
            &state.first_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_REGISTRATION_GRANTED,
            json!({}),
            json!({
                "authority_kind": "ens_v2_registry",
                "authority_key": format!(
                    "ens-v2-registry:{}:{}:{}",
                    state.first_ref.chain_id, state.registry_contract_instance_id, link.upstream_resource
                ),
                "registrant": state.owner,
                "expiry": state.expiry,
                "labelhash": state.labelhash,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
                "status": "registered",
            }),
            format!("registration-granted:{}", link.upstream_resource),
        ));
        events.push(normalized_event(
            &state.first_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_AUTHORITY_TRANSFERRED,
            json!({}),
            json!({
                "owner": state.owner,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
            }),
            format!("authority-transferred:{}", link.upstream_resource),
        ));
    }
    if let Some(expiry) = state.expiry {
        events.push(normalized_event(
            &state.current_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_EXPIRY_CHANGED,
            json!({}),
            json!({
                "expiry": expiry,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
            }),
            format!("expiry-current:{}", link.upstream_resource),
        ));
    }
    events
}

async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: &ObservationRef,
) -> Result<NameSurface> {
    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(NameSurface {
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
                "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(NameSurface {
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
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "logical_name_id": name.logical_name_id,
        }),
        canonicality_state: reference.canonicality_state,
    })
}

async fn build_token_lineage(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<TokenLineage> {
    if let Some(existing) =
        load_token_lineage_including_noncanonical(pool, link.token_lineage_id).await?
    {
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: token_lineage_provenance(state, link),
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id: link.token_lineage_id,
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: token_lineage_provenance(state, link),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

async fn build_resource(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<Resource> {
    if let Some(existing) = load_resource_including_noncanonical(pool, link.resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(Some(link.token_lineage_id)),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: resource_provenance(state, link),
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id: link.resource_id,
        token_lineage_id: Some(link.token_lineage_id),
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: resource_provenance(state, link),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

async fn build_surface_binding(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, link.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: existing.active_to,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from: event_position_timestamp(&link.linked_ref),
        active_to: None,
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "binding_kind": state.binding_kind.as_str(),
            "logical_name_id": state.name.logical_name_id,
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

fn token_lineage_provenance(state: &RegistryNameState, link: &RegistryResourceLink) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
        "chain_id": link.linked_ref.chain_id,
        "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        "registry_address": state.registry_address,
        "upstream_resource": link.upstream_resource,
        "token_id": link.observed_token_id,
        "current_token_id": state.token_id,
        "logical_name_id": state.name.logical_name_id,
    })
}

fn resource_provenance(state: &RegistryNameState, link: &RegistryResourceLink) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
        "chain_id": link.linked_ref.chain_id,
        "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        "registry_address": state.registry_address,
        "upstream_resource": link.upstream_resource,
        "token_id": link.observed_token_id,
        "current_token_id": state.token_id,
        "logical_name_id": state.name.logical_name_id,
        "labelhash": state.labelhash,
        "source_family": state.source_family,
        "source_manifest_id": state.source_manifest_id,
        "manifest_version": state.manifest_version,
    })
}

fn build_registry_observation(raw_log: &RegistryRawLogRow) -> Result<Option<RegistryObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let reference = raw_log.reference();

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelRegistered missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelRegistered missing labelHash topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("LabelRegistered missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        let owner = decode_address_word(&raw_log.data, 1)?;
        let expiry = decode_u64_word(&raw_log.data, 2)?;
        return Ok(Some(RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_RESERVED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelReserved missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelReserved missing labelHash topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("LabelReserved missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        let expiry = decode_u64_word(&raw_log.data, 1)?;
        return Ok(Some(RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_UNREGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelUnregistered missing tokenId topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("LabelUnregistered missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(EXPIRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ExpiryUpdated missing tokenId topic")?,
        )?;
        let new_expiry = decode_u64_topic(
            raw_log
                .topics
                .get(2)
                .context("ExpiryUpdated missing newExpiry topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("ExpiryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(SUBREGISTRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("SubregistryUpdated missing tokenId topic")?,
        )?;
        let subregistry = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("SubregistryUpdated missing subregistry topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("SubregistryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(RESOLVER_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ResolverUpdated missing tokenId topic")?,
        )?;
        let resolver = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("ResolverUpdated missing resolver topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("ResolverUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_RESOURCE_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenResource missing tokenId topic")?,
        )?;
        let upstream_resource = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenResource missing resource topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_REGENERATED_SIGNATURE)) {
        let old_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenRegenerated missing oldTokenId topic")?,
        )?;
        let new_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenRegenerated missing newTokenId topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(PARENT_UPDATED_SIGNATURE)) {
        let parent = normalize_topic_address(
            raw_log
                .topics
                .get(1)
                .context("ParentUpdated missing parent topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("ParentUpdated missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        return Ok(Some(RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
        }));
    }

    Ok(None)
}

fn normalized_event(
    reference: &ObservationRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: Value,
    after_state: Value,
    identity_suffix: String,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "ens_v2_registry_resource_surface:{}:{}:{}:{}:{}:{}",
            reference.source_manifest_id,
            reference.block_hash,
            reference.transaction_hash,
            reference.log_index,
            event_kind,
            identity_suffix
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
        transaction_hash: Some(reference.transaction_hash.clone()),
        log_index: Some(reference.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "transaction_hash": reference.transaction_hash,
            "transaction_index": reference.transaction_index,
            "log_index": reference.log_index,
            "emitting_address": reference.emitting_address,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE.to_owned(),
        canonicality_state: reference.canonicality_state,
        before_state,
        after_state,
    }
}

async fn load_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
) -> Result<Vec<RegistryRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let emitters_by_address = emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rb.block_timestamp,
            rl.transaction_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address,
            rl.topics,
            rl.data,
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
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 registry raw logs for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let emitting_address = normalize_address(
                &row.try_get::<String, _>("emitting_address")
                    .context("missing emitting_address")?,
            );
            let emitter = emitters_by_address
                .get(&emitting_address)
                .with_context(|| {
                    format!(
                        "missing ENSv2 registry emitter attribution for chain {chain} address {emitting_address}"
                    )
                })?;
            Ok(RegistryRawLogRow {
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
                emitting_address,
                topics: row.try_get("topics").context("missing topics")?,
                data: row.try_get("data").context("missing data")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                emitting_contract_instance_id: emitter.contract_instance_id,
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
        .context("failed to load watched contracts for ENSv2 registry adapter")?;
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
        let source_manifest_id = watched_contract
            .source_manifest_id
            .context("watched contract missing source_manifest_id after validation")?;
        let manifest = active_manifests.get(&source_manifest_id).with_context(|| {
            format!("missing active manifest metadata for manifest_id {source_manifest_id}")
        })?;
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_ROOT_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        {
            continue;
        }
        if manifest.chain != watched_contract.chain {
            bail!(
                "watched contract chain {} does not match active manifest chain {} for manifest_id {}",
                watched_contract.chain,
                manifest.chain,
                source_manifest_id
            );
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            role: manifest.role.clone(),
            source: watched_contract.source,
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
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (mv.manifest_id)
            mv.manifest_id,
            mv.chain,
            mv.namespace,
            mv.source_family,
            mv.manifest_version,
            mv.normalizer_version,
            mci.role
        FROM manifest_versions mv
        LEFT JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.declaration_kind = 'contract'
        WHERE mv.rollout_status = 'active'
          AND mv.manifest_id = ANY($1::BIGINT[])
        ORDER BY mv.manifest_id, mci.manifest_contract_instance_id
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv2 registry emitters")?;

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
                role: row.try_get("role").context("missing role")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

impl RegistryRawLogRow {
    fn reference(&self) -> ObservationRef {
        ObservationRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            block_timestamp: self.block_timestamp,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            emitting_address: self.emitting_address.clone(),
            emitting_contract_instance_id: self.emitting_contract_instance_id,
            canonicality_state: self.canonicality_state,
            namespace: self.namespace.clone(),
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}

fn initial_registry_suffixes(emitters: &[ActiveEmitter]) -> HashMap<String, String> {
    let mut suffixes = HashMap::new();
    for emitter in emitters {
        if emitter.source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1 {
            suffixes.insert(emitter.address.clone(), String::new());
        } else if emitter.source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
            && emitter.source != WatchedContractSource::DiscoveryEdge
        {
            suffixes.insert(emitter.address.clone(), "eth".to_owned());
        }
    }
    suffixes
}

fn name_under_registry(
    registry_address: &str,
    label: &str,
    registry_suffix_by_address: &HashMap<String, String>,
) -> Option<String> {
    let normalized_label = label.trim().to_ascii_lowercase();
    if normalized_label.is_empty() {
        return None;
    }
    let suffix = registry_suffix_by_address.get(registry_address)?;
    if suffix.is_empty() {
        Some(normalized_label)
    } else {
        Some(format!("{normalized_label}.{suffix}"))
    }
}

fn observe_name(
    namespace: &str,
    full_name: &str,
    reference: &ObservationRef,
    label: &str,
) -> Result<NameMetadata> {
    let normalized_name = full_name.to_ascii_lowercase();
    let labels = normalized_name
        .split('.')
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let dns_encoded_name = dns_encode(&labels)?;
    let labelhashes = labels
        .iter()
        .map(|label| format!("0x{}", hex_string(keccak256_bytes(label))))
        .collect::<Vec<_>>();
    Ok(NameMetadata {
        namespace: namespace.to_owned(),
        logical_name_id: format!("{namespace}:{normalized_name}"),
        input_name: normalized_name.clone(),
        canonical_display_name: display_name(full_name),
        normalized_name,
        dns_encoded_name,
        namehash: format!("0x{}", hex_string(namehash_bytes(&labels))),
        labelhashes,
        normalizer_version: reference
            .source_family
            .starts_with("ens_v2")
            .then(|| "uts46-v1".to_owned())
            .unwrap_or_else(|| label.to_owned()),
    })
}

fn state_for_token_mut<'a>(
    states: &'a mut BTreeMap<(String, String), RegistryNameState>,
    aliases: &HashMap<(String, String), (String, String)>,
    registry: &str,
    token_id: &str,
) -> Option<&'a mut RegistryNameState> {
    let key = resolve_token_key(aliases, registry, token_id)
        .unwrap_or_else(|| (registry.to_owned(), token_id.to_owned()));
    states.get_mut(&key)
}

fn resolve_token_key(
    aliases: &HashMap<(String, String), (String, String)>,
    registry: &str,
    token_id: &str,
) -> Option<(String, String)> {
    aliases
        .get(&(registry.to_owned(), token_id.to_owned()))
        .cloned()
}

fn remember_linked_resource_state(
    linked_resource_states: &mut BTreeMap<Uuid, RegistryNameState>,
    state: &RegistryNameState,
) {
    if let Some(link) = state.resource.as_ref() {
        linked_resource_states.insert(link.resource_id, state.clone());
    }
}

fn closed_surface_binding_for_unregister(
    state: &RegistryNameState,
    reference: &ObservationRef,
) -> Option<SurfaceBinding> {
    let link = state.resource.as_ref()?;
    Some(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from: event_position_timestamp(&link.linked_ref),
        active_to: Some(event_position_timestamp(reference)),
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "binding_kind": state.binding_kind.as_str(),
            "logical_name_id": state.name.logical_name_id,
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        canonicality_state: reference.canonicality_state,
    })
}

fn deactivate_registry_suffix(
    registry_suffix_by_address: &mut HashMap<String, String>,
    registry_address: Option<&str>,
    expected_suffix: &str,
) {
    let Some(registry_address) = registry_address else {
        return;
    };
    if registry_address == ZERO_ADDRESS {
        return;
    }
    if registry_suffix_by_address
        .get(registry_address)
        .is_some_and(|suffix| suffix == expected_suffix)
    {
        registry_suffix_by_address.remove(registry_address);
    }
}

fn event_position_timestamp(reference: &ObservationRef) -> OffsetDateTime {
    let offset_micros = reference
        .transaction_index
        .saturating_mul(1_000)
        .saturating_add(reference.log_index.max(0));
    reference.block_timestamp + Duration::from_micros(offset_micros.max(0) as u64)
}

fn latest_discovery_observations(
    observations: Vec<DiscoveryObservation>,
) -> Result<Vec<DiscoveryObservation>> {
    let mut latest = BTreeMap::<String, DiscoveryObservation>::new();
    for observation in observations {
        let key = observation
            .provenance
            .get("observation_key")
            .and_then(Value::as_str)
            .context("ENSv2 discovery observation missing observation_key")?
            .to_owned();
        latest.insert(key, observation);
    }
    Ok(latest.into_values().collect())
}

async fn reconcile_discovery_observations_by_source(
    pool: &PgPool,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    let mut by_source = BTreeMap::<String, Vec<DiscoveryObservation>>::new();
    for observation in observations {
        by_source
            .entry(observation.discovery_source.clone())
            .or_default()
            .push(observation.clone());
    }

    let mut summary = DiscoveryReconciliationSummary {
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        admitted_edges: Vec::new(),
    };
    for (discovery_source, source_observations) in by_source {
        let source_summary =
            reconcile_discovery_observations(pool, &discovery_source, &source_observations)
                .await
                .with_context(|| {
                    format!("failed to reconcile discovery_source {discovery_source}")
                })?;
        summary.active_edge_count += source_summary.active_edge_count;
        summary.admitted_edge_count += source_summary.admitted_edge_count;
        summary.inserted_edge_count += source_summary.inserted_edge_count;
        summary.deactivated_edge_count += source_summary.deactivated_edge_count;
        summary.admitted_edges.extend(source_summary.admitted_edges);
    }
    Ok(summary)
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (
        candidate.source_rank,
        candidate.source_manifest_id,
        candidate.contract_instance_id,
    ) < (
        current.source_rank,
        current.source_manifest_id,
        current.contract_instance_id,
    )
}

fn ens_v2_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_subregistry:{chain}")
}

fn ens_v2_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_resolver:{chain}")
}

fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    let offset = decode_usize_word(data, offset_word_index)?;
    if data.len() < offset + 32 {
        bail!("dynamic string payload is missing length word");
    }
    let length = decode_usize_at(data, offset)?;
    let start = offset + 32;
    let end = start + length;
    if data.len() < end {
        bail!("dynamic string payload is shorter than declared length");
    }
    String::from_utf8(data[start..end].to_vec()).context("dynamic string is not valid UTF-8")
}

fn decode_address_word(data: &[u8], word_index: usize) -> Result<String> {
    let word = word_at(data, word_index)?;
    Ok(format!("0x{}", hex_string(&word[12..32])))
}

fn decode_u64_word(data: &[u8], word_index: usize) -> Result<i64> {
    let word = word_at(data, word_index)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("u64 ABI word exceeds supported width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("u64 ABI word does not fit in i64")
}

fn decode_usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    let word = word_at(data, word_index)?;
    decode_usize(word)
}

fn decode_usize_at(data: &[u8], offset: usize) -> Result<usize> {
    if data.len() < offset + 32 {
        bail!("ABI word offset is outside payload");
    }
    decode_usize(&data[offset..offset + 32])
}

fn decode_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

fn word_at(data: &[u8], word_index: usize) -> Result<&[u8]> {
    let start = word_index
        .checked_mul(32)
        .context("ABI word index overflow")?;
    let end = start + 32;
    data.get(start..end)
        .with_context(|| format!("ABI data missing word {word_index}"))
}

fn decode_u64_topic(value: &str) -> Result<i64> {
    let bytes = decode_hex_32(value)?;
    if bytes[..24].iter().any(|byte| *byte != 0) {
        bail!("indexed u64 topic exceeds supported width");
    }
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&bytes[24..32]);
    i64::try_from(u64::from_be_bytes(tail)).context("indexed u64 topic does not fit in i64")
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

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized[2..].as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex chunk must be UTF-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn null_if_zero_address(value: &str) -> Value {
    if normalize_address(value) == ZERO_ADDRESS {
        Value::Null
    } else {
        Value::String(normalize_address(value))
    }
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

fn dns_encode(labels: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for label in labels {
        let length = u8::try_from(label.len()).context("label exceeds DNS label length")?;
        if length == 0 {
            bail!("empty label is not encodable");
        }
        output.push(length);
        output.extend_from_slice(label);
    }
    output.push(0);
    Ok(output)
}

fn display_name(name: &str) -> String {
    let mut labels = name.split('.');
    let Some(first) = labels.next() else {
        return name.to_owned();
    };
    let mut first_chars = first.chars();
    let display_first = match first_chars.next() {
        Some(first_char) => format!(
            "{}{}",
            first_char.to_uppercase(),
            first_chars.as_str().to_ascii_lowercase()
        ),
        None => first.to_owned(),
    };
    std::iter::once(display_first)
        .chain(labels.map(|label| label.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join(".")
}

fn namehash_bytes(labels: &[Vec<u8>]) -> [u8; 32] {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    node
}

fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
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

fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use bigname_storage::{default_database_url, load_surface_bindings_by_logical_name_id};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

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
                .context("failed to parse database URL for ENSv2 registry tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_ens_v2_registry_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for ENSv2 registry tests")?;
            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for ENSv2 registry tests")?;
            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for ENSv2 registry tests")?;

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

    #[test]
    fn ens_v2_token_regeneration_preserves_resource_identity() -> Result<()> {
        let registry = "0x00000000000000000000000000000000000000aa".to_owned();
        let contract_instance_id = Uuid::from_u128(0x1234);
        let old_token_id =
            "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
        let new_token_id =
            "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
        let upstream_resource =
            "0x0000000000000000000000000000000000000000000000000000000000000eac".to_owned();

        let mut registry_suffix_by_address =
            HashMap::from([(registry.clone(), "alice.eth".to_owned())]);
        let mut registry_contract_by_address =
            HashMap::from([(registry.clone(), contract_instance_id)]);
        let mut states_by_registry_token = BTreeMap::new();
        let mut linked_resource_states = BTreeMap::new();
        let mut closed_bindings = BTreeMap::new();
        let mut token_aliases = HashMap::new();
        let mut observations = Vec::new();
        let mut graph_events = Vec::new();

        apply_registry_observation(
            RegistryObservation::LabelRegistered {
                token_id: old_token_id.clone(),
                labelhash: "0x0000000000000000000000000000000000000000000000000000000000000b0b"
                    .to_owned(),
                label: "bob".to_owned(),
                owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
                expiry: 1_900_000_000,
                sender: "0x0000000000000000000000000000000000000dad".to_owned(),
                reference: reference(&registry, contract_instance_id, 10, 0),
            },
            &mut registry_suffix_by_address,
            &mut registry_contract_by_address,
            &mut states_by_registry_token,
            &mut linked_resource_states,
            &mut closed_bindings,
            &mut token_aliases,
            &mut observations,
            &mut graph_events,
        )?;
        apply_registry_observation(
            RegistryObservation::TokenResource {
                token_id: old_token_id.clone(),
                upstream_resource: upstream_resource.clone(),
                reference: reference(&registry, contract_instance_id, 10, 1),
            },
            &mut registry_suffix_by_address,
            &mut registry_contract_by_address,
            &mut states_by_registry_token,
            &mut linked_resource_states,
            &mut closed_bindings,
            &mut token_aliases,
            &mut observations,
            &mut graph_events,
        )?;
        apply_registry_observation(
            RegistryObservation::TokenRegenerated {
                old_token_id: old_token_id.clone(),
                new_token_id: new_token_id.clone(),
                reference: reference(&registry, contract_instance_id, 11, 0),
            },
            &mut registry_suffix_by_address,
            &mut registry_contract_by_address,
            &mut states_by_registry_token,
            &mut linked_resource_states,
            &mut closed_bindings,
            &mut token_aliases,
            &mut observations,
            &mut graph_events,
        )?;

        let state = states_by_registry_token
            .get(&(registry.clone(), old_token_id.clone()))
            .context("state should remain keyed by the original token observation")?;
        let link = state
            .resource
            .as_ref()
            .context("TokenResource should link a stable EAC resource")?;
        assert_eq!(state.token_id, new_token_id);
        assert_eq!(
            link.resource_id,
            deterministic_uuid(&format!(
                "ens-v2-resource:{}:{}:{}",
                "ethereum-sepolia", contract_instance_id, upstream_resource
            ))
        );
        assert!(graph_events.iter().any(|event| {
            event.event_kind == EVENT_KIND_TOKEN_REGENERATED
                && event.resource_id == Some(link.resource_id)
                && event.after_state["new_token_id"] == Value::String(new_token_id.clone())
        }));
        let linked_state = linked_resource_states
            .get(&link.resource_id)
            .context("linked resource state should track regenerated token")?;
        let linked_event = build_resource_events(
            linked_state,
            linked_state
                .resource
                .as_ref()
                .context("linked state should keep resource")?,
        )
        .into_iter()
        .find(|event| event.event_kind == EVENT_KIND_TOKEN_RESOURCE_LINKED)
        .context("TokenResourceLinked event should be emitted")?;
        assert_eq!(
            linked_event.after_state["token_id"],
            Value::String(old_token_id.clone())
        );
        assert_eq!(
            linked_event.after_state["current_token_id"],
            Value::String(new_token_id.clone())
        );

        Ok(())
    }

    #[test]
    fn ens_v2_unregister_closes_binding_before_reregistering_new_resource() -> Result<()> {
        let registry = "0x00000000000000000000000000000000000000aa".to_owned();
        let contract_instance_id = Uuid::from_u128(0x1234);
        let first_token =
            "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
        let second_token =
            "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
        let first_resource =
            "0x0000000000000000000000000000000000000000000000000000000000000ea1".to_owned();
        let second_resource =
            "0x0000000000000000000000000000000000000000000000000000000000000ea2".to_owned();
        let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

        harness.apply(RegistryObservation::LabelRegistered {
            token_id: first_token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 10, 0),
        })?;
        harness.apply(RegistryObservation::TokenResource {
            token_id: first_token.clone(),
            upstream_resource: first_resource.clone(),
            reference: reference(&registry, contract_instance_id, 10, 1),
        })?;
        harness.apply(RegistryObservation::LabelUnregistered {
            token_id: first_token.clone(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 11, 0),
        })?;
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: second_token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a22".to_owned(),
            expiry: 2_000_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 12, 0),
        })?;
        harness.apply(RegistryObservation::TokenResource {
            token_id: second_token.clone(),
            upstream_resource: second_resource.clone(),
            reference: reference(&registry, contract_instance_id, 12, 1),
        })?;

        let first_resource_id = deterministic_uuid(&format!(
            "ens-v2-resource:{}:{}:{}",
            "ethereum-sepolia", contract_instance_id, first_resource
        ));
        let second_resource_id = deterministic_uuid(&format!(
            "ens-v2-resource:{}:{}:{}",
            "ethereum-sepolia", contract_instance_id, second_resource
        ));
        assert!(
            harness
                .linked_resource_states
                .contains_key(&first_resource_id)
        );
        assert!(
            harness
                .linked_resource_states
                .contains_key(&second_resource_id)
        );
        let closed_binding = harness
            .closed_bindings
            .values()
            .find(|binding| binding.resource_id == first_resource_id)
            .context("unregister should close the first resource binding")?;
        assert_eq!(closed_binding.logical_name_id, "ens:alice.eth".to_owned());
        assert_eq!(
            closed_binding.active_to,
            Some(
                OffsetDateTime::from_unix_timestamp(1_717_172_711)
                    .expect("test timestamp should fit")
            )
        );
        let second_link = harness
            .linked_resource_states
            .get(&second_resource_id)
            .and_then(|state| state.resource.as_ref())
            .context("second registration should have a resource link")?;
        assert!(closed_binding.active_to.is_some_and(
            |active_to| active_to <= event_position_timestamp(&second_link.linked_ref)
        ));
        assert_ne!(first_resource_id, second_resource_id);

        Ok(())
    }

    #[tokio::test]
    async fn ens_v2_unregister_reregister_upserts_close_before_open_successor() -> Result<()> {
        let database = TestDatabase::new().await?;
        let registry = "0x00000000000000000000000000000000000000aa".to_owned();
        let contract_instance_id = Uuid::from_u128(0x1234);
        let first_token =
            "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
        let second_token =
            "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
        let first_resource =
            "0x0000000000000000000000000000000000000000000000000000000000000ea1".to_owned();
        let second_resource =
            "0x0000000000000000000000000000000000000000000000000000000000000ea2".to_owned();
        let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

        harness.apply(RegistryObservation::LabelRegistered {
            token_id: first_token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 10, 0),
        })?;
        harness.apply(RegistryObservation::TokenResource {
            token_id: first_token.clone(),
            upstream_resource: first_resource.clone(),
            reference: reference(&registry, contract_instance_id, 10, 1),
        })?;

        let first_resource_id = deterministic_uuid(&format!(
            "ens-v2-resource:{}:{}:{}",
            "ethereum-sepolia", contract_instance_id, first_resource
        ));
        let first_state = harness
            .linked_resource_states
            .get(&first_resource_id)
            .cloned()
            .context("first resource state should be linked")?;
        let first_link = first_state
            .resource
            .as_ref()
            .cloned()
            .context("first state should hold resource link")?;
        upsert_token_lineages(
            database.pool(),
            &[build_token_lineage(database.pool(), &first_state, &first_link).await?],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[build_resource(database.pool(), &first_state, &first_link).await?],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[
                build_name_surface(database.pool(), &first_state.name, &first_state.first_ref)
                    .await?,
            ],
        )
        .await?;
        let old_open_binding = build_surface_binding(database.pool(), &first_state, &first_link)
            .await
            .context("first open binding should build")?;
        upsert_surface_bindings(database.pool(), &[old_open_binding])
            .await
            .context("old open binding should persist")?;

        harness.apply(RegistryObservation::LabelUnregistered {
            token_id: first_token.clone(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 11, 0),
        })?;
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: second_token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a22".to_owned(),
            expiry: 2_000_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 12, 0),
        })?;
        harness.apply(RegistryObservation::TokenResource {
            token_id: second_token.clone(),
            upstream_resource: second_resource.clone(),
            reference: reference(&registry, contract_instance_id, 12, 1),
        })?;

        let second_resource_id = deterministic_uuid(&format!(
            "ens-v2-resource:{}:{}:{}",
            "ethereum-sepolia", contract_instance_id, second_resource
        ));
        let second_state = harness
            .linked_resource_states
            .get(&second_resource_id)
            .cloned()
            .context("second resource state should be linked")?;
        let second_link = second_state
            .resource
            .as_ref()
            .cloned()
            .context("second state should hold resource link")?;
        upsert_token_lineages(
            database.pool(),
            &[build_token_lineage(database.pool(), &second_state, &second_link).await?],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[build_resource(database.pool(), &second_state, &second_link).await?],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[
                build_name_surface(database.pool(), &second_state.name, &second_state.first_ref)
                    .await?,
            ],
        )
        .await?;

        let closed_old_binding = harness
            .closed_bindings
            .get(&first_link.surface_binding_id)
            .cloned()
            .context("unregister should close old binding")?;
        let new_open_binding = build_surface_binding(database.pool(), &second_state, &second_link)
            .await
            .context("second open binding should build")?;
        upsert_surface_bindings_close_before_open(
            database.pool(),
            &[new_open_binding.clone(), closed_old_binding.clone()],
        )
        .await
        .context("ordered lifecycle binding upsert should close old before opening successor")?;

        let stored = load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth")
            .await
            .context("stored bindings should load")?;
        assert_eq!(stored.len(), 2);
        let old = stored
            .iter()
            .find(|binding| binding.resource_id == first_resource_id)
            .context("old binding should remain stored")?;
        let new = stored
            .iter()
            .find(|binding| binding.resource_id == second_resource_id)
            .context("new binding should be stored")?;
        assert!(old.active_to.is_some());
        assert!(new.active_to.is_none());
        assert!(
            old.active_to
                .is_some_and(|active_to| active_to <= new.active_from)
        );

        database.cleanup().await
    }

    #[test]
    fn ens_v2_subregistry_change_omits_unadmitted_endpoint_id() -> Result<()> {
        let registry = "0x00000000000000000000000000000000000000aa".to_owned();
        let child = "0x00000000000000000000000000000000000000c1".to_owned();
        let contract_instance_id = Uuid::from_u128(0x1234);
        let token = "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
        let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

        harness.apply(RegistryObservation::LabelRegistered {
            token_id: token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 10, 0),
        })?;
        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: token,
            subregistry: child,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 11, 0),
        })?;

        let event = harness
            .graph_events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
            .context("SubregistryChanged should be emitted")?;
        assert_eq!(event.after_state["to_contract_instance_id"], Value::Null);
        assert!(
            !harness
                .registry_contract_by_address
                .contains_key("0x00000000000000000000000000000000000000c1")
        );

        Ok(())
    }

    #[test]
    fn ens_v2_subregistry_zero_and_swap_deactivate_stale_child_suffixes() -> Result<()> {
        let registry = "0x00000000000000000000000000000000000000aa".to_owned();
        let child_one = "0x00000000000000000000000000000000000000c1".to_owned();
        let child_two = "0x00000000000000000000000000000000000000c2".to_owned();
        let contract_instance_id = Uuid::from_u128(0x1234);
        let child_instance_id = Uuid::from_u128(0x5678);
        let parent_token =
            "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
        let child_token =
            "0x00000000000000000000000000000000000000000000000000000000000000b1".to_owned();
        let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

        harness.apply(RegistryObservation::LabelRegistered {
            token_id: parent_token.clone(),
            labelhash: labelhash("alice"),
            label: "alice".to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 10, 0),
        })?;
        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: parent_token.clone(),
            subregistry: child_one.clone(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 11, 0),
        })?;
        assert_eq!(
            harness.registry_suffix_by_address.get(&child_one),
            Some(&"alice.eth".to_owned())
        );

        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: parent_token.clone(),
            subregistry: ZERO_ADDRESS.to_owned(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 12, 0),
        })?;
        assert!(!harness.registry_suffix_by_address.contains_key(&child_one));
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: child_token.clone(),
            labelhash: labelhash("bob"),
            label: "bob".to_owned(),
            owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&child_one, child_instance_id, 13, 0),
        })?;
        assert!(
            harness
                .states_by_registry_token
                .get(&(child_one.clone(), child_token.clone()))
                .is_none()
        );

        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: parent_token,
            subregistry: child_two.clone(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, 14, 0),
        })?;
        assert!(!harness.registry_suffix_by_address.contains_key(&child_one));
        assert_eq!(
            harness.registry_suffix_by_address.get(&child_two),
            Some(&"alice.eth".to_owned())
        );
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: child_token.clone(),
            labelhash: labelhash("bob"),
            label: "bob".to_owned(),
            owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&child_one, child_instance_id, 15, 0),
        })?;
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: child_token.clone(),
            labelhash: labelhash("bob"),
            label: "bob".to_owned(),
            owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&child_two, child_instance_id, 16, 0),
        })?;
        assert!(
            harness
                .states_by_registry_token
                .get(&(child_one, child_token.clone()))
                .is_none()
        );
        assert!(
            harness
                .states_by_registry_token
                .get(&(child_two, child_token))
                .is_some()
        );

        Ok(())
    }

    struct RegistryHarness {
        registry_suffix_by_address: HashMap<String, String>,
        registry_contract_by_address: HashMap<String, Uuid>,
        states_by_registry_token: BTreeMap<(String, String), RegistryNameState>,
        linked_resource_states: BTreeMap<Uuid, RegistryNameState>,
        closed_bindings: BTreeMap<Uuid, SurfaceBinding>,
        token_aliases: HashMap<(String, String), (String, String)>,
        observations: Vec<DiscoveryObservation>,
        graph_events: Vec<NormalizedEvent>,
    }

    impl RegistryHarness {
        fn new(registry: &str, contract_instance_id: Uuid, suffix: &str) -> Self {
            Self {
                registry_suffix_by_address: HashMap::from([(
                    registry.to_owned(),
                    suffix.to_owned(),
                )]),
                registry_contract_by_address: HashMap::from([(
                    registry.to_owned(),
                    contract_instance_id,
                )]),
                states_by_registry_token: BTreeMap::new(),
                linked_resource_states: BTreeMap::new(),
                closed_bindings: BTreeMap::new(),
                token_aliases: HashMap::new(),
                observations: Vec::new(),
                graph_events: Vec::new(),
            }
        }

        fn apply(&mut self, observation: RegistryObservation) -> Result<()> {
            apply_registry_observation(
                observation,
                &mut self.registry_suffix_by_address,
                &mut self.registry_contract_by_address,
                &mut self.states_by_registry_token,
                &mut self.linked_resource_states,
                &mut self.closed_bindings,
                &mut self.token_aliases,
                &mut self.observations,
                &mut self.graph_events,
            )
        }
    }

    fn labelhash(label: &str) -> String {
        format!("0x{}", hex_string(keccak256_bytes(label.as_bytes())))
    }

    fn reference(
        registry: &str,
        contract_instance_id: Uuid,
        block_number: i64,
        log_index: i64,
    ) -> ObservationRef {
        ObservationRef {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: format!("0xblock{block_number}"),
            block_number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_700 + block_number)
                .expect("test timestamp should fit"),
            transaction_hash: format!("0xtx{block_number}"),
            transaction_index: 0,
            log_index,
            emitting_address: registry.to_owned(),
            emitting_contract_instance_id: contract_instance_id,
            canonicality_state: CanonicalityState::Finalized,
            namespace: "ens".to_owned(),
            source_manifest_id: 1,
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            manifest_version: 1,
        }
    }
}
