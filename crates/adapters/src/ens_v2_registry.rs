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

    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(pool, chain, true, block_hashes, None)
            .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
        )
        .await
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
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
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
struct RegistryRawLogSourceScopeTarget {
    source_family: String,
    address: String,
    effective_from_block: i64,
    effective_to_block: i64,
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
    sync_ens_v2_registry_resource_surface_with_scope(pool, chain, false, &[], None).await
}

async fn sync_ens_v2_registry_resource_surface_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    let source_scope = source_scope.map(normalized_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
    }
    let scoped_emitter_identities = source_scope.as_ref().map(|source_scope| {
        source_scope
            .iter()
            .map(|target| (target.source_family.clone(), target.address.clone()))
            .collect::<HashSet<_>>()
    });

    let active_emitters =
        load_active_emitters(pool, chain, scoped_emitter_identities.as_ref()).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
    }

    let raw_logs = load_registry_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope.as_deref(),
    )
    .await?;
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
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(observation, &mut context)?;
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

struct RegistryObservationContext<'a> {
    registry_suffix_by_address: &'a mut HashMap<String, String>,
    registry_contract_by_address: &'a mut HashMap<String, Uuid>,
    states_by_registry_token: &'a mut BTreeMap<(String, String), RegistryNameState>,
    linked_resource_states: &'a mut BTreeMap<Uuid, RegistryNameState>,
    closed_bindings: &'a mut BTreeMap<Uuid, SurfaceBinding>,
    token_aliases: &'a mut HashMap<(String, String), (String, String)>,
    observations: &'a mut Vec<DiscoveryObservation>,
    graph_events: &'a mut Vec<NormalizedEvent>,
}

fn apply_registry_observation(
    observation: RegistryObservation,
    context: &mut RegistryObservationContext<'_>,
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
                context.registry_suffix_by_address,
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
            context.graph_events.push(normalized_event(
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
                    "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    "resource_pending": true,
                }),
                format!("label-registered:{}", state.token_id),
            ));
            context.states_by_registry_token.insert(key, state);
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
                context.registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let name = observe_name(&reference.namespace, &full_name, &reference, &label)?;
            context.states_by_registry_token.insert(
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
            context.graph_events.push(normalized_event(
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
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                if let Some(binding) = closed_surface_binding_for_unregister(state, &reference) {
                    context
                        .closed_bindings
                        .insert(binding.surface_binding_id, binding);
                }
                state.status = "unregistered";
                state.current_ref = reference.clone();
                context.graph_events.push(normalized_event(
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
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
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
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before_expiry = state.expiry;
                state.expiry = Some(new_expiry);
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
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
                context.graph_events.push(normalized_event(
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
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
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
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.subregistry.clone();
                if before.as_deref() != Some(subregistry.as_str()) {
                    deactivate_registry_suffix(
                        context.registry_suffix_by_address,
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
                    context
                        .registry_suffix_by_address
                        .insert(subregistry.clone(), state.full_name.clone());
                }
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
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
                        "to_contract_instance_id": context.registry_contract_by_address
                            .get(&subregistry)
                            .map(ToString::to_string),
                    }),
                    format!("subregistry-updated:{token_id}"),
                ));
            }
            context.observations.push(DiscoveryObservation {
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
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.resolver.clone();
                state.resolver = Some(resolver.clone());
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
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
                context.observations.push(DiscoveryObservation {
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
                context.states_by_registry_token,
                context.token_aliases,
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
                remember_linked_resource_state(context.linked_resource_states, state);
            }
        }
        RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        } => {
            let canonical_key = resolve_token_key(
                context.token_aliases,
                &reference.emitting_address,
                &old_token_id,
            )
            .unwrap_or_else(|| (reference.emitting_address.clone(), old_token_id.clone()));
            if let Some(state) = context.states_by_registry_token.get_mut(&canonical_key) {
                let previous_token_id = state.token_id.clone();
                state.token_id = new_token_id.clone();
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.token_aliases.insert(
                    (reference.emitting_address.clone(), old_token_id.clone()),
                    canonical_key.clone(),
                );
                context.token_aliases.insert(
                    (reference.emitting_address.clone(), new_token_id.clone()),
                    canonical_key,
                );
                context.graph_events.push(normalized_event(
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
                name_under_registry(&parent, &label, context.registry_suffix_by_address)
            {
                context
                    .registry_suffix_by_address
                    .insert(reference.emitting_address.clone(), full_name.clone());
                context.graph_events.push(normalized_event(
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
                        "parent_contract_instance_id": context.registry_contract_by_address
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
                "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
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
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Result<Vec<RegistryRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let watched_range_addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let watched_range_from_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let watched_range_to_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();

    let scoped_ranges = source_scope
        .map(|source_scope| scoped_ranges_for_active_emitters(source_scope, emitters))
        .transpose()?;
    let rows = if let Some(scoped_ranges) = scoped_ranges.as_ref() {
        if scoped_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let scoped_addresses = scoped_ranges
            .iter()
            .map(|target| target.address.clone())
            .collect::<Vec<_>>();
        let scoped_from_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_from_block)
            .collect::<Vec<_>>();
        let scoped_to_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_to_block)
            .collect::<Vec<_>>();

        sqlx::query(
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
              AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
              AND EXISTS (
                  SELECT 1
                  FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE watched.address = lower(rl.emitting_address)
                    AND rl.block_number BETWEEN watched.effective_from_block
                        AND watched.effective_to_block
              )
              AND EXISTS (
                  SELECT 1
                  FROM unnest($8::TEXT[], $9::BIGINT[], $10::BIGINT[]) AS scoped(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE scoped.address = lower(rl.emitting_address)
                    AND rl.block_number BETWEEN scoped.effective_from_block
                        AND scoped.effective_to_block
              )
              AND rl.canonicality_state <> 'orphaned'::canonicality_state
            ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .bind(&watched_range_addresses)
        .bind(&watched_range_from_blocks)
        .bind(&watched_range_to_blocks)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load scoped ENSv2 registry raw logs for chain {chain}")
        })?
    } else {
        sqlx::query(
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
              AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
              AND rl.canonicality_state <> 'orphaned'::canonicality_state
            ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load ENSv2 registry raw logs for chain {chain}"))?
    };

    let mut output = Vec::new();
    for row in rows {
        let emitting_address = normalize_address(
            &row.try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?,
        );
        let block_number = row
            .try_get("block_number")
            .context("missing block_number")?;
        let Some(emitter) = emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| emitter_for_block_and_scope(emitters, block_number, source_scope))
        else {
            continue;
        };
        output.push(RegistryRawLogRow {
            chain_id: row.try_get("chain_id").context("missing chain_id")?,
            block_hash: row.try_get("block_hash").context("missing block_hash")?,
            block_number,
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
        });
    }
    Ok(output)
}

async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    scoped_emitter_identities: Option<&HashSet<(String, String)>>,
) -> Result<Vec<ActiveEmitter>> {
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

    let mut emitter_candidates = Vec::new();
    for watched_contract in watched_contracts {
        let source_manifest_id = watched_contract
            .source_manifest_id
            .context("watched contract missing source_manifest_id after validation")?;
        let manifest = active_manifests.get(&source_manifest_id).with_context(|| {
            format!("missing active manifest metadata for manifest_id {source_manifest_id}")
        })?;
        if scoped_emitter_identities.is_some_and(|scope| {
            !scope.contains(&(
                manifest.source_family.clone(),
                normalize_address(&watched_contract.address),
            ))
        }) {
            continue;
        }
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
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
        };

        emitter_candidates.push(candidate);
    }

    Ok(preferred_emitters_by_scope(emitter_candidates))
}

fn source_scope_target_intersects_active_emitter(
    target: &RegistryRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    if target.source_family != emitter.source_family || target.address != emitter.address {
        return false;
    }

    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);

    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

fn emitter_sort_key(
    emitter: &ActiveEmitter,
) -> (&str, &str, Option<i64>, Option<i64>, i32, i64, Uuid) {
    (
        &emitter.source_family,
        &emitter.address,
        emitter.active_from_block_number,
        emitter.active_to_block_number,
        emitter.source_rank,
        emitter.source_manifest_id,
        emitter.contract_instance_id,
    )
}

fn sort_emitters_by_scope(emitters: &mut [ActiveEmitter]) {
    emitters.sort_by(|left, right| emitter_sort_key(left).cmp(&emitter_sort_key(right)));
}

fn preferred_emitters_by_scope(
    candidates: impl IntoIterator<Item = ActiveEmitter>,
) -> Vec<ActiveEmitter> {
    let mut emitters_by_scope =
        HashMap::<(String, String, Option<i64>, Option<i64>), ActiveEmitter>::new();
    for candidate in candidates {
        let scope_key = (
            candidate.source_family.clone(),
            candidate.address.clone(),
            candidate.active_from_block_number,
            candidate.active_to_block_number,
        );
        match emitters_by_scope.get(&scope_key) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_scope.insert(scope_key, candidate);
            }
        }
    }

    let mut emitters = emitters_by_scope.into_values().collect::<Vec<_>>();
    sort_emitters_by_scope(&mut emitters);
    emitters
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
        normalizer_version: if reference.source_family.starts_with("ens_v2") {
            "uts46-v1".to_owned()
        } else {
            label.to_owned()
        },
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

fn normalized_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<RegistryRawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                RegistryRawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: normalize_address(address),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                }
            },
        )
        .collect()
}

fn scoped_ranges_for_active_emitters(
    source_scope: &[RegistryRawLogSourceScopeTarget],
    emitters: &[ActiveEmitter],
) -> Result<Vec<RegistryRawLogSourceScopeTarget>> {
    let mut ranges = Vec::new();
    for target in source_scope {
        if target.effective_to_block < target.effective_from_block {
            bail!(
                "ENSv2 registry source scope range {}..={} is invalid for {} {}",
                target.effective_from_block,
                target.effective_to_block,
                target.source_family,
                target.address
            );
        }
        if emitters
            .iter()
            .any(|emitter| source_scope_target_intersects_active_emitter(target, emitter))
        {
            ranges.push(target.clone());
        }
    }
    Ok(ranges)
}

fn emitter_for_block_and_scope<'a>(
    emitters: &'a [ActiveEmitter],
    block_number: i64,
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Option<&'a ActiveEmitter> {
    let Some(source_scope) = source_scope else {
        return emitters
            .iter()
            .find(|emitter| emitter_active_at_block(emitter, block_number));
    };

    emitters.iter().find(|emitter| {
        emitter_active_at_block(emitter, block_number)
            && source_scope.iter().any(|target| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && block_number >= target.effective_from_block
                    && block_number <= target.effective_to_block
            })
    })
}

fn emitter_active_at_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|active_from| block_number >= active_from)
        && emitter
            .active_to_block_number
            .is_none_or(|active_to| block_number <= active_to)
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
    for (index, chunk) in normalized.as_bytes()[2..].chunks(2).enumerate() {
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
mod tests;
