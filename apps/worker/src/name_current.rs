use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, HistoryEvent, HistoryScope, NameCurrentRow, SurfaceBindingKind,
    clear_name_current, delete_name_current, load_name_history_head,
    load_surface_bindings_by_logical_name_id, upsert_name_current_rows,
};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

const ENS_NAMESPACE: &str = "ens";
const BASENAMES_NAMESPACE: &str = "basenames";
const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENS_V2_REGISTRAR_DERIVATION_KIND: &str = "ens_v2_registrar";
const ENS_V2_RESOLVER_DERIVATION_KIND: &str = "ens_v2_resolver";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
const SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH: &str = "ens_v2_sepolia_dev";
const CAPABILITY_STATUS_SUPPORTED: &str = "supported";
const MANIFEST_ROLLOUT_STATUS_ACTIVE: &str = "active";
const ETHEREUM_SEPOLIA_CHAIN_ID: &str = "ethereum-sepolia";
const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const SOURCE_FAMILY_BASENAMES_EXECUTION: &str = "basenames_execution";
const VERIFIED_RESOLUTION_CAPABILITY: &str = "verified_resolution";
const BASENAMES_V1_DEPLOYMENT_EPOCH: &str = "basenames_v1";
const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";
const NAME_CURRENT_DERIVATION_KIND: &str = "name_current_rebuild";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "record_inventory remains unsupported in the ENSv1 name_current rebuild";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const RELEVANT_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    EVENT_KIND_ALIAS_CHANGED,
    "ExpiryChanged",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    EVENT_KIND_RECORD_VERSION_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED,
    "SurfaceBound",
    "SurfaceUnbound",
    "TokenResourceLinked",
    "TokenRegenerated",
    "TokenControlTransferred",
];
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentRebuildSummary {
    pub requested_name_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug)]
struct NameSurfaceSeed {
    logical_name_id: String,
    namespace: String,
    canonical_display_name: String,
    normalized_name: String,
    namehash: String,
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: Option<OffsetDateTime>,
    canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
struct CurrentBindingContext {
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    binding_kind: SurfaceBindingKind,
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: Option<OffsetDateTime>,
    surface_binding_state: CanonicalityState,
    resource_state: CanonicalityState,
    token_lineage_state: Option<CanonicalityState>,
}

#[derive(Clone, Debug)]
struct RelevantEvent {
    normalized_event_id: i64,
    resource_id: Option<Uuid>,
    event_kind: String,
    source_family: String,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    source_manifest_version: Option<i64>,
    source_manifest_namespace: Option<String>,
    source_manifest_source_family: Option<String>,
    source_manifest_chain: Option<String>,
    source_manifest_deployment_epoch: Option<String>,
    source_manifest_rollout_status: Option<String>,
    exact_name_profile_status: Option<String>,
    chain_id: Option<String>,
    block_number: Option<i64>,
    block_hash: Option<String>,
    block_timestamp: Option<OffsetDateTime>,
    raw_fact_ref: Value,
    canonicality_state: CanonicalityState,
    after_state: Value,
}

#[derive(Clone, Debug, Default)]
struct ProjectedFacts {
    registration_status: Option<String>,
    authority_kind: Option<String>,
    authority_key: Option<String>,
    registrant: Option<String>,
    expiry: Option<i64>,
    released_at: Option<i64>,
    registry_owner: Option<String>,
    latest_registration_event_kind: Option<String>,
    latest_control_event_kind: Option<String>,
    control_status_substrate: Option<String>,
    control_expiry_substrate: Option<i64>,
    resolver_chain_id: Option<String>,
    resolver_address: Option<String>,
    latest_resolver_event_kind: Option<String>,
    surface_head: Option<HistoryPointer>,
    resource_head: Option<HistoryPointer>,
}

#[derive(Clone, Debug)]
struct ChainPositionCandidate {
    slot: String,
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: OffsetDateTime,
}

#[derive(Clone, Debug)]
struct SupplementalChainObservation {
    candidate: ChainPositionCandidate,
    canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
struct SupportedResolutionProjection {
    topology: Value,
    manifest_versions: Vec<Value>,
}

#[derive(Clone, Debug)]
struct BasenamesExecutionManifestVersion {
    manifest_version: i64,
    chain: String,
    deployment_epoch: String,
    contract_address: String,
}

#[derive(Clone, Debug)]
struct WildcardSourceContext {
    logical_name_id: String,
    namespace: String,
    normalized_name: String,
    canonical_display_name: String,
    namehash: String,
    resource_id: Uuid,
    resolver_event: RelevantEvent,
    boundary_event: RelevantEvent,
    matched_labels: Vec<String>,
}

impl WildcardSourceContext {
    fn events(&self) -> impl Iterator<Item = &RelevantEvent> {
        let mut events = vec![&self.resolver_event];
        if self.boundary_event.normalized_event_id != self.resolver_event.normalized_event_id {
            events.push(&self.boundary_event);
        }
        events.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
struct HistoryHeads {
    surface_head: Option<HistoryEvent>,
    resource_head: Option<HistoryEvent>,
}

impl HistoryHeads {
    fn iter(&self) -> impl Iterator<Item = &HistoryEvent> {
        self.surface_head.iter().chain(self.resource_head.iter())
    }
}

#[derive(Clone, Debug)]
struct HistoryPointer {
    normalized_event_id: i64,
    event_kind: String,
    chain_position: Value,
}

pub async fn rebuild_name_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<NameCurrentRebuildSummary> {
    match logical_name_id {
        Some(logical_name_id) => rebuild_one_name_current(pool, logical_name_id).await,
        None => rebuild_all_name_current(pool).await,
    }
}

async fn rebuild_all_name_current(pool: &PgPool) -> Result<NameCurrentRebuildSummary> {
    let names = load_canonical_name_surfaces(pool).await?;
    let mut rows = Vec::with_capacity(names.len());
    for name in &names {
        rows.push(build_name_current_row(pool, name).await?);
    }

    let upserted_row_count = upsert_name_current_rows(pool, &rows).await?.len();
    let logical_name_ids = rows
        .iter()
        .map(|row| row.logical_name_id.clone())
        .collect::<Vec<_>>();
    let deleted_row_count = delete_stale_name_current_rows(pool, &logical_name_ids).await?;
    Ok(NameCurrentRebuildSummary {
        requested_name_count: names.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_name_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<NameCurrentRebuildSummary> {
    let Some(name) = load_canonical_name_surface(pool, logical_name_id).await? else {
        let deleted_row_count = delete_name_current(pool, logical_name_id).await?;
        return Ok(NameCurrentRebuildSummary {
            requested_name_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let row = build_name_current_row(pool, &name).await?;
    let upserted_row_count = upsert_name_current_rows(pool, &[row]).await?.len();
    Ok(NameCurrentRebuildSummary {
        requested_name_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn delete_stale_name_current_rows(pool: &PgPool, logical_name_ids: &[String]) -> Result<u64> {
    if logical_name_ids.is_empty() {
        return clear_name_current(pool).await;
    }

    sqlx::query(
        r#"
        DELETE FROM name_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[]) AS replacement(logical_name_id)
            WHERE replacement.logical_name_id = current.logical_name_id
        )
        "#,
    )
    .bind(logical_name_ids)
    .execute(pool)
    .await
    .context("failed to delete stale name_current rows after rebuild")
    .map(|result| result.rows_affected())
}

async fn build_name_current_row(pool: &PgPool, name: &NameSurfaceSeed) -> Result<NameCurrentRow> {
    let current_binding = load_current_binding_context(pool, &name.logical_name_id).await?;
    let events = load_relevant_events(pool, name).await?;
    let history_heads = load_history_heads(pool, &name.logical_name_id).await?;
    let basenames_execution_manifest =
        load_active_basenames_execution_manifest(pool, &name.namespace).await?;
    let wildcard_source_context =
        load_wildcard_source_context(pool, name, current_binding.as_ref()).await?;
    let supplemental_chain_observations = load_supplemental_chain_observations(
        pool,
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )
    .await?;
    let facts = project_facts(&events, current_binding.as_ref(), &history_heads)?;
    let chain_positions = build_chain_positions(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let supported_resolution_projection = build_supported_resolution_projection(
        name,
        current_binding.as_ref(),
        &facts,
        &events,
        &chain_positions,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )?;
    let canonicality_summary = build_canonicality_summary(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let provenance = build_provenance(
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        supported_resolution_projection
            .as_ref()
            .map(|projection| projection.manifest_versions.as_slice())
            .unwrap_or(&[]),
    )?;
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .chain(
            wildcard_source_context
                .as_ref()
                .into_iter()
                .flat_map(WildcardSourceContext::events)
                .map(|event| event.manifest_version),
        )
        .chain(history_heads.iter().map(|event| event.manifest_version))
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    )
    .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    Ok(NameCurrentRow {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        namehash: name.namehash.clone(),
        surface_binding_id: current_binding
            .as_ref()
            .map(|binding| binding.surface_binding_id),
        resource_id: current_binding.as_ref().map(|binding| binding.resource_id),
        token_lineage_id: current_binding
            .as_ref()
            .and_then(|binding| binding.token_lineage_id),
        binding_kind: current_binding.as_ref().map(|binding| binding.binding_kind),
        declared_summary: build_declared_summary(
            facts,
            supported_resolution_projection.map(|projection| projection.topology),
        ),
        provenance,
        coverage: build_exact_name_coverage(&name.namespace, &events),
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    })
}

fn build_declared_summary(facts: ProjectedFacts, topology: Option<Value>) -> Value {
    let surface_head = facts
        .surface_head
        .as_ref()
        .map(history_pointer_json)
        .unwrap_or(Value::Null);
    let resource_head = facts
        .resource_head
        .as_ref()
        .map(history_pointer_json)
        .unwrap_or(Value::Null);

    let mut summary = Map::new();
    summary.insert(
        "registration".to_owned(),
        json!({
            "status": facts.registration_status,
            "authority_kind": facts.authority_kind,
            "authority_key": facts.authority_key,
            "registrant": facts.registrant,
            "expiry": facts.expiry,
            "released_at": facts.released_at,
            "latest_event_kind": facts.latest_registration_event_kind,
        }),
    );
    summary.insert(
        "control".to_owned(),
        json!({
            "status": facts.control_status_substrate,
            "expiry": format_unix_timestamp_value(facts.control_expiry_substrate),
            "registrant": facts.registrant,
            "registry_owner": facts.registry_owner,
            "latest_event_kind": facts.latest_control_event_kind,
        }),
    );
    summary.insert(
        "resolver".to_owned(),
        json!({
            "chain_id": facts.resolver_chain_id,
            "address": facts.resolver_address,
            "latest_event_kind": facts.latest_resolver_event_kind,
        }),
    );
    summary.insert(
        "record_inventory".to_owned(),
        json!({
            "status": "unsupported",
            "unsupported_reason": RECORD_INVENTORY_UNSUPPORTED_REASON,
        }),
    );
    summary.insert(
        "history".to_owned(),
        json!({
            "surface_head": surface_head,
            "resource_head": resource_head,
        }),
    );
    if let Some(topology) = topology {
        summary.insert("topology".to_owned(), topology);
    }

    Value::Object(summary)
}

async fn load_active_basenames_execution_manifest(
    pool: &PgPool,
    namespace: &str,
) -> Result<Option<BasenamesExecutionManifestVersion>> {
    if namespace != BASENAMES_NAMESPACE {
        return Ok(None);
    }

    let row = sqlx::query(
        r#"
        SELECT
            mv.manifest_version,
            mv.chain,
            mv.deployment_epoch,
            mci.declared_address AS contract_address
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf
          ON mcf.manifest_id = mv.manifest_id
         AND mcf.capability_name = $1
         AND mcf.status = 'supported'::capability_support_status
        JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.declaration_kind = 'contract'
         AND mci.role = 'l1_resolver'
         AND lower(mci.declared_address) = lower($6)
        WHERE mv.namespace = $2
          AND mv.source_family = $3
          AND mv.chain = $4
          AND mv.deployment_epoch = $5
          AND mv.rollout_status = 'active'::manifest_rollout_status
        ORDER BY mv.manifest_version DESC, mv.manifest_id DESC
        LIMIT 1
        "#,
    )
    .bind(VERIFIED_RESOLUTION_CAPABILITY)
    .bind(BASENAMES_NAMESPACE)
    .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(BASENAMES_V1_DEPLOYMENT_EPOCH)
    .bind(BASENAMES_L1_RESOLVER_ADDRESS)
    .fetch_optional(pool)
    .await
    .context("failed to load active basenames_execution manifest metadata for name_current")?;

    row.map(|row| {
        Ok(BasenamesExecutionManifestVersion {
            manifest_version: row
                .try_get("manifest_version")
                .context("missing basenames_execution manifest_version")?,
            chain: row
                .try_get("chain")
                .context("missing basenames_execution chain")?,
            deployment_epoch: row
                .try_get("deployment_epoch")
                .context("missing basenames_execution deployment_epoch")?,
            contract_address: row
                .try_get("contract_address")
                .context("missing basenames_execution contract_address")?,
        })
    })
    .transpose()
}

async fn load_supplemental_chain_observations(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    wildcard_source_context: Option<&WildcardSourceContext>,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Vec<SupplementalChainObservation>> {
    let mut observations = Vec::new();

    if let Some(context) = wildcard_source_context {
        for event in context.events() {
            if let Some(observation) = supplemental_chain_observation_from_event(event)? {
                observations.push(observation);
            }
        }
    }

    if let Some(observation) = load_basenames_execution_target_lineage_observation(
        pool,
        name,
        current_binding,
        events,
        history_heads,
        basenames_execution_manifest,
    )
    .await?
    {
        observations.push(observation);
    }

    Ok(observations)
}

fn supplemental_chain_observation_from_event(
    event: &RelevantEvent,
) -> Result<Option<SupplementalChainObservation>> {
    let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
        event.chain_id.as_ref(),
        event.block_number,
        event.block_hash.as_ref(),
        event.block_timestamp,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupplementalChainObservation {
        candidate: ChainPositionCandidate {
            slot: chain_slot(chain_id),
            chain_id: chain_id.clone(),
            block_number,
            block_hash: block_hash.clone(),
            timestamp,
        },
        canonicality_state: event.canonicality_state,
    }))
}

async fn load_basenames_execution_target_lineage_observation(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupplementalChainObservation>> {
    if name.namespace != BASENAMES_NAMESPACE || basenames_execution_manifest.is_none() {
        return Ok(None);
    }
    if current_binding
        .is_none_or(|binding| binding.binding_kind != SurfaceBindingKind::DeclaredRegistryPath)
    {
        return Ok(None);
    }

    let Some(base_boundary) = latest_chain_position_for_chain(
        name,
        current_binding,
        events,
        history_heads,
        BASE_MAINNET_CHAIN_ID,
    ) else {
        return Ok(None);
    };

    let row = sqlx::query(&format!(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#
    ))
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(base_boundary.timestamp)
    .fetch_optional(pool)
    .await
    .context("failed to load Basenames execution target lineage position for name_current")?;

    row.map(|row| {
        let chain_id = row
            .try_get::<String, _>("chain_id")
            .context("missing Basenames transport chain_id")?;
        Ok(SupplementalChainObservation {
            candidate: ChainPositionCandidate {
                slot: chain_slot(&chain_id),
                chain_id,
                block_number: row
                    .try_get("block_number")
                    .context("missing Basenames transport block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("missing Basenames transport block_hash")?,
                timestamp: row
                    .try_get("block_timestamp")
                    .context("missing Basenames transport block_timestamp")?,
            },
            canonicality_state: parse_canonicality_state(
                &row.try_get::<String, _>("canonicality_state")
                    .context("missing Basenames transport canonicality_state")?,
            )?,
        })
    })
    .transpose()
}

fn latest_chain_position_for_chain(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    chain_id: &str,
) -> Option<ChainPositionCandidate> {
    let mut latest_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if name.chain_id == chain_id
        && let Some(timestamp) = name.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&name.chain_id),
                chain_id: name.chain_id.clone(),
                block_number: name.block_number,
                block_hash: name.block_hash.clone(),
                timestamp,
            },
        );
    }

    if let Some(binding) = current_binding
        && binding.chain_id == chain_id
        && let Some(timestamp) = binding.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.chain_id),
                chain_id: binding.chain_id.clone(),
                block_number: binding.block_number,
                block_hash: binding.block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in events {
        let (Some(event_chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        if event_chain_id != chain_id {
            continue;
        }
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(event_chain_id),
                chain_id: event_chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in history_heads.iter() {
        let (Some(event_chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        if event_chain_id != chain_id {
            continue;
        }
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(event_chain_id),
                chain_id: event_chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    latest_positions.into_values().next()
}

fn build_supported_resolution_projection(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
    wildcard_source_context: Option<&WildcardSourceContext>,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(binding) = current_binding else {
        return Ok(None);
    };

    match name.namespace.as_str() {
        ENS_NAMESPACE => match binding.binding_kind {
            SurfaceBindingKind::ResolverAliasPath => {
                build_alias_only_supported_projection(name, binding, facts, events, chain_positions)
            }
            SurfaceBindingKind::ObservedWildcardPath => {
                build_wildcard_supported_projection(name, binding, wildcard_source_context)
            }
            _ => Ok(None),
        },
        BASENAMES_NAMESPACE => build_basenames_supported_projection(
            name,
            binding,
            facts,
            events,
            chain_positions,
            basenames_execution_manifest,
        ),
        _ => Ok(None),
    }
}

fn build_alias_only_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(final_target) = events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_ALIAS_CHANGED
                && event.after_state.get("active").and_then(Value::as_bool) == Some(true)
        })
        .and_then(alias_final_target_ref)
    else {
        return Ok(None);
    };
    let Some(resolver_hop) = resolver_hop_from_facts(
        &name.logical_name_id,
        &name.namespace,
        &name.normalized_name,
        &name.canonical_display_name,
        current_binding.resource_id,
        facts,
    ) else {
        return Ok(None);
    };
    let Some(boundary) = build_supported_resolution_boundary_from_chain_positions(
        chain_positions,
        &name.logical_name_id,
        current_binding.resource_id,
        None,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::ResolverAliasPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": empty_wildcard_detail(),
            "alias": {
                "final_target": final_target.clone(),
                "hops": [final_target],
            },
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": empty_transport_detail(),
        }),
        manifest_versions: Vec::new(),
    }))
}

fn build_wildcard_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    wildcard_source_context: Option<&WildcardSourceContext>,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(source_context) = wildcard_source_context else {
        return Ok(None);
    };
    let Some(boundary) = build_supported_resolution_boundary_from_event(
        &source_context.logical_name_id,
        source_context.resource_id,
        &source_context.boundary_event,
    ) else {
        return Ok(None);
    };
    let Some(resolver_hop) = resolver_hop_from_event(source_context) else {
        return Ok(None);
    };
    let source = wildcard_source_ref(source_context);
    let matched_labels = source_context
        .matched_labels
        .iter()
        .map(|label| Value::String(label.clone()))
        .collect::<Vec<_>>();

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::ObservedWildcardPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": {
                "source": source,
                "matched_labels": matched_labels,
            },
            "alias": empty_alias_detail(),
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": empty_transport_detail(),
        }),
        manifest_versions: Vec::new(),
    }))
}

fn build_basenames_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupportedResolutionProjection>> {
    if current_binding.binding_kind != SurfaceBindingKind::DeclaredRegistryPath {
        return Ok(None);
    }
    let Some(manifest) = basenames_execution_manifest else {
        return Ok(None);
    };
    if manifest.chain != ETHEREUM_MAINNET_CHAIN_ID
        || !manifest
            .contract_address
            .eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
        || !chain_positions_include_chain(chain_positions, BASE_MAINNET_CHAIN_ID)
        || !chain_positions_include_chain(chain_positions, ETHEREUM_MAINNET_CHAIN_ID)
    {
        return Ok(None);
    }
    let Some(resolver_hop) = resolver_hop_from_facts(
        &name.logical_name_id,
        &name.namespace,
        &name.normalized_name,
        &name.canonical_display_name,
        current_binding.resource_id,
        facts,
    ) else {
        return Ok(None);
    };
    if facts.resolver_chain_id.as_deref() != Some(BASE_MAINNET_CHAIN_ID) {
        return Ok(None);
    }
    let Some(boundary) = build_basenames_supported_boundary(
        &name.logical_name_id,
        current_binding.resource_id,
        events,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::DeclaredRegistryPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": empty_wildcard_detail(),
            "alias": empty_alias_detail(),
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": {
                "source_chain_id": BASE_MAINNET_CHAIN_ID,
                "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                "latest_event_kind": Value::Null,
            },
        }),
        manifest_versions: vec![basenames_execution_manifest_value(manifest)],
    }))
}

fn basenames_execution_manifest_value(manifest: &BasenamesExecutionManifestVersion) -> Value {
    json!({
        "source_family": SOURCE_FAMILY_BASENAMES_EXECUTION,
        "manifest_version": manifest.manifest_version,
        "chain": manifest.chain,
        "deployment_epoch": manifest.deployment_epoch,
    })
}

fn build_basenames_supported_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    events: &[RelevantEvent],
) -> Option<Value> {
    let boundary_anchor = events.iter().rev().find(|event| {
        event.resource_id == Some(resource_id)
            && event.chain_id.as_deref() == Some(BASE_MAINNET_CHAIN_ID)
            && matches!(
                event.event_kind.as_str(),
                EVENT_KIND_RECORD_VERSION_CHANGED | EVENT_KIND_RESOLVER_CHANGED
            )
    })?;
    let chain_position = relevant_event_chain_position(boundary_anchor)?;
    let has_pointer = boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": has_pointer.then_some(boundary_anchor.normalized_event_id),
        "event_kind": has_pointer.then_some(boundary_anchor.event_kind.clone()),
        "chain_position": chain_position,
    }))
}

fn build_supported_resolution_boundary_from_chain_positions(
    chain_positions: &Value,
    logical_name_id: &str,
    resource_id: Uuid,
    preferred_chain_id: Option<&str>,
) -> Option<Value> {
    let chain_position = preferred_chain_id
        .and_then(|chain_id| chain_position_for_chain(chain_positions, chain_id))
        .or_else(|| chain_position_slot(chain_positions, "ethereum"))
        .or_else(|| only_chain_position(chain_positions))?;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": Value::Null,
        "event_kind": Value::Null,
        "chain_position": chain_position,
    }))
}

fn build_supported_resolution_boundary_from_event(
    logical_name_id: &str,
    resource_id: Uuid,
    event: &RelevantEvent,
) -> Option<Value> {
    let chain_position = relevant_event_chain_position(event)?;
    let has_pointer = event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": has_pointer.then_some(event.normalized_event_id),
        "event_kind": has_pointer.then_some(event.event_kind.clone()),
        "chain_position": chain_position,
    }))
}

fn alias_final_target_ref(event: &RelevantEvent) -> Option<Value> {
    let logical_name_id = json_str(&event.after_state, &["to_logical_name_id"]).or_else(|| {
        json_str(&event.after_state, &["to_name"])
            .map(|name| format!("{ENS_NAMESPACE}:{}", name.to_ascii_lowercase()))
    })?;
    let normalized_name = json_str(&event.after_state, &["to_normalized_name"]).or_else(|| {
        json_str(&event.after_state, &["to_name"]).map(|name| name.to_ascii_lowercase())
    })?;
    let canonical_display_name = json_str(&event.after_state, &["to_canonical_display_name"])
        .or_else(|| json_str(&event.after_state, &["to_name"]))?;
    let namehash = json_str(&event.after_state, &["to_namehash"])?;
    let resource_id = json_str(&event.after_state, &["to_resource_id"])?;

    Some(json!({
        "logical_name_id": logical_name_id,
        "namespace": ENS_NAMESPACE,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "namehash": namehash,
        "resource_id": resource_id,
        "binding_kind": SurfaceBindingKind::ResolverAliasPath.as_str(),
    }))
}

fn wildcard_source_ref(source_context: &WildcardSourceContext) -> Value {
    name_ref(
        &source_context.logical_name_id,
        &source_context.namespace,
        &source_context.normalized_name,
        &source_context.canonical_display_name,
        &source_context.namehash,
        source_context.resource_id,
        SurfaceBindingKind::ObservedWildcardPath,
    )
}

fn resolver_hop_from_event(source_context: &WildcardSourceContext) -> Option<Value> {
    Some(json!({
        "logical_name_id": source_context.logical_name_id,
        "namespace": source_context.namespace,
        "normalized_name": source_context.normalized_name,
        "canonical_display_name": source_context.canonical_display_name,
        "resource_id": source_context.resource_id.to_string(),
        "chain_id": source_context.resolver_event.chain_id.as_ref()?,
        "address": normalize_resolver_address(json_str(&source_context.resolver_event.after_state, &["resolver"]).as_deref())?,
        "latest_event_kind": source_context.resolver_event.event_kind,
    }))
}

fn resolver_hop_from_facts(
    logical_name_id: &str,
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    facts: &ProjectedFacts,
) -> Option<Value> {
    Some(json!({
        "logical_name_id": logical_name_id,
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "resource_id": resource_id.to_string(),
        "chain_id": facts.resolver_chain_id.as_ref()?,
        "address": facts.resolver_address.as_ref()?,
        "latest_event_kind": facts.latest_resolver_event_kind.clone(),
    }))
}

fn name_ref(
    logical_name_id: &str,
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    namehash: &str,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "namehash": namehash,
        "resource_id": resource_id.to_string(),
        "binding_kind": binding_kind.as_str(),
    })
}

fn empty_alias_detail() -> Value {
    json!({
        "final_target": Value::Null,
        "hops": [],
    })
}

fn empty_wildcard_detail() -> Value {
    json!({
        "source": Value::Null,
        "matched_labels": [],
    })
}

fn empty_transport_detail() -> Value {
    json!({
        "source_chain_id": Value::Null,
        "target_chain_id": Value::Null,
        "contract_address": Value::Null,
        "latest_event_kind": Value::Null,
    })
}

fn chain_positions_include_chain(chain_positions: &Value, chain_id: &str) -> bool {
    chain_position_for_chain(chain_positions, chain_id).is_some()
}

fn chain_position_for_chain(chain_positions: &Value, chain_id: &str) -> Option<Value> {
    chain_positions
        .as_object()?
        .values()
        .find(|position| {
            position
                .get("chain_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value == chain_id)
        })
        .cloned()
}

fn chain_position_slot(chain_positions: &Value, slot: &str) -> Option<Value> {
    chain_positions.as_object()?.get(slot).cloned()
}

fn only_chain_position(chain_positions: &Value) -> Option<Value> {
    let positions = chain_positions.as_object()?;
    if positions.len() == 1 {
        positions.values().next().cloned()
    } else {
        None
    }
}

fn relevant_event_chain_position(event: &RelevantEvent) -> Option<Value> {
    Some(json!({
        "chain_id": event.chain_id.as_ref()?,
        "block_number": event.block_number?,
        "block_hash": event.block_hash.as_ref()?,
        "timestamp": format_timestamp(event.block_timestamp?),
    }))
}

fn build_provenance(
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    wildcard_source_context: Option<&WildcardSourceContext>,
    supplemental_manifest_versions: &[Value],
) -> Result<Value> {
    let mut normalized_event_ids = Vec::new();
    let mut seen_normalized_event_ids = BTreeSet::new();
    for normalized_event_id in events
        .iter()
        .map(|event| event.normalized_event_id)
        .chain(
            wildcard_source_context
                .into_iter()
                .flat_map(WildcardSourceContext::events)
                .map(|event| event.normalized_event_id),
        )
        .chain(history_heads.iter().map(|event| event.normalized_event_id))
    {
        if seen_normalized_event_ids.insert(normalized_event_id) {
            normalized_event_ids.push(normalized_event_id);
        }
    }

    let raw_fact_refs = dedupe_json_values(
        events
            .iter()
            .map(|event| event.raw_fact_ref.clone())
            .chain(
                wildcard_source_context
                    .into_iter()
                    .flat_map(WildcardSourceContext::events)
                    .map(|event| event.raw_fact_ref.clone()),
            )
            .chain(history_heads.iter().map(|event| event.raw_fact_ref.clone())),
    )?;
    let manifest_versions = dedupe_json_values(
        events
            .iter()
            .map(event_manifest_version)
            .chain(
                wildcard_source_context
                    .into_iter()
                    .flat_map(WildcardSourceContext::events)
                    .map(event_manifest_version),
            )
            .chain(history_heads.iter().map(history_manifest_version)),
    )?;
    let manifest_versions = dedupe_json_values(
        manifest_versions
            .into_iter()
            .chain(supplemental_manifest_versions.iter().cloned()),
    )?;

    Ok(json!({
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": NAME_CURRENT_DERIVATION_KIND,
    }))
}

fn build_chain_positions(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Value {
    let mut latest_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    if let Some(timestamp) = name.block_timestamp {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&name.chain_id),
                chain_id: name.chain_id.clone(),
                block_number: name.block_number,
                block_hash: name.block_hash.clone(),
                timestamp,
            },
        );
    }

    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(&binding.chain_id),
                chain_id: binding.chain_id.clone(),
                block_number: binding.block_number,
                block_hash: binding.block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in events {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for event in history_heads.iter() {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            event.chain_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
            event.block_timestamp,
        ) else {
            continue;
        };
        push_chain_position(
            &mut latest_positions,
            ChainPositionCandidate {
                slot: chain_slot(chain_id),
                chain_id: chain_id.clone(),
                block_number,
                block_hash: block_hash.clone(),
                timestamp,
            },
        );
    }

    for observation in supplemental_chain_observations {
        push_chain_position(&mut latest_positions, observation.candidate.clone());
    }

    Value::Object(
        latest_positions
            .into_iter()
            .map(|(slot, position)| {
                (
                    slot,
                    json!({
                        "chain_id": position.chain_id,
                        "block_number": position.block_number,
                        "block_hash": position.block_hash,
                        "timestamp": format_timestamp(position.timestamp),
                    }),
                )
            })
            .collect(),
    )
}

fn push_chain_position(
    latest_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    let replace = latest_positions
        .get(&candidate.slot)
        .map(|current| {
            candidate.block_number > current.block_number
                || (candidate.block_number == current.block_number
                    && candidate.block_hash > current.block_hash)
        })
        .unwrap_or(true);
    if replace {
        latest_positions.insert(candidate.slot.clone(), candidate);
    }
}

fn build_canonicality_summary(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Value {
    let mut states = vec![name.canonicality_state];
    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    merge_chain_state(&mut chain_states, &name.chain_id, name.canonicality_state);

    if let Some(binding) = current_binding {
        states.push(binding.surface_binding_state);
        states.push(binding.resource_state);
        merge_chain_state(
            &mut chain_states,
            &binding.chain_id,
            binding.surface_binding_state,
        );
        merge_chain_state(&mut chain_states, &binding.chain_id, binding.resource_state);
        if let Some(token_lineage_state) = binding.token_lineage_state {
            states.push(token_lineage_state);
            merge_chain_state(&mut chain_states, &binding.chain_id, token_lineage_state);
        }
    }

    for event in events {
        states.push(event.canonicality_state);
        if let Some(chain_id) = event.chain_id.as_ref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    for event in history_heads.iter() {
        states.push(event.canonicality_state);
        if let Some(chain_id) = event.chain_id.as_ref() {
            merge_chain_state(&mut chain_states, chain_id, event.canonicality_state);
        }
    }

    for observation in supplemental_chain_observations {
        states.push(observation.canonicality_state);
        merge_chain_state(
            &mut chain_states,
            &observation.candidate.chain_id,
            observation.canonicality_state,
        );
    }

    let status =
        weakest_canonicality(states.iter().copied()).unwrap_or(CanonicalityState::Canonical);
    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn merge_chain_state(
    chain_states: &mut BTreeMap<String, CanonicalityState>,
    chain_id: &str,
    state: CanonicalityState,
) {
    let replacement = chain_states
        .get(chain_id)
        .map(|current| canonicality_rank(state) < canonicality_rank(*current))
        .unwrap_or(true);
    if replacement {
        chain_states.insert(chain_id.to_owned(), state);
    }
}

fn project_facts(
    events: &[RelevantEvent],
    current_binding: Option<&CurrentBindingContext>,
    history_heads: &HistoryHeads,
) -> Result<ProjectedFacts> {
    let mut facts = ProjectedFacts::default();

    for event in events {
        if let Some(status) = json_str(&event.after_state, &["status"]) {
            facts.control_status_substrate = Some(status);
        }
        if let Some(expiry) = json_i64(&event.after_state, &["expiry"]) {
            facts.control_expiry_substrate = Some(expiry);
        }

        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                facts.registration_status = Some("active".to_owned());
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.registrant = json_str(&event.after_state, &["registrant"]);
                facts.expiry = json_i64(&event.after_state, &["expiry"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationRenewed" => {
                if facts.registration_status.as_deref() != Some("released") {
                    facts.registration_status = Some("active".to_owned());
                }
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "ExpiryChanged" => {
                facts.expiry = json_i64(&event.after_state, &["expiry"]).or(facts.expiry);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "RegistrationReleased" => {
                facts.registration_status = Some("released".to_owned());
                facts.released_at = json_i64(&event.after_state, &["released_at"]);
                facts.latest_registration_event_kind = Some(event.event_kind.clone());
            }
            "TokenControlTransferred" => {
                facts.registrant = json_str(&event.after_state, &["to"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityTransferred" => {
                facts.registry_owner = json_str(&event.after_state, &["owner"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            "AuthorityEpochChanged" => {
                facts.authority_kind = json_str(&event.after_state, &["authority_kind"]);
                facts.authority_key = json_str(&event.after_state, &["authority_key"]);
                facts.latest_control_event_kind = Some(event.event_kind.clone());
            }
            EVENT_KIND_RESOLVER_CHANGED
                if current_binding.map(|binding| binding.resource_id) == event.resource_id =>
            {
                let resolver_address = normalize_resolver_address(
                    json_str(&event.after_state, &["resolver"]).as_deref(),
                );
                if resolver_address.is_some() && event.chain_id.is_none() {
                    bail!(
                        "ResolverChanged event {} for logical_name_id {} is missing chain_id",
                        event.normalized_event_id,
                        current_binding
                            .map(|binding| binding.surface_binding_id.to_string())
                            .unwrap_or_default()
                    );
                }
                facts.resolver_chain_id = resolver_address
                    .as_ref()
                    .and_then(|_| event.chain_id.clone());
                facts.resolver_address = resolver_address;
                facts.latest_resolver_event_kind = Some(event.event_kind.clone());
            }
            _ => {}
        }
    }

    if current_binding.is_some() && facts.registration_status.is_none() {
        facts.registration_status = Some("active".to_owned());
    }

    facts.surface_head = history_heads
        .surface_head
        .as_ref()
        .map(history_pointer_from_event);
    facts.resource_head = history_heads
        .resource_head
        .as_ref()
        .map(history_pointer_from_event);

    Ok(facts)
}

fn max_timestamp(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    supplemental_chain_observations: &[SupplementalChainObservation],
) -> Option<OffsetDateTime> {
    let mut timestamps = Vec::new();
    if let Some(timestamp) = name.block_timestamp {
        timestamps.push(timestamp);
    }
    if let Some(binding) = current_binding
        && let Some(timestamp) = binding.block_timestamp
    {
        timestamps.push(timestamp);
    }
    timestamps.extend(events.iter().filter_map(|event| event.block_timestamp));
    timestamps.extend(
        history_heads
            .iter()
            .filter_map(|event| event.block_timestamp),
    );
    timestamps.extend(
        supplemental_chain_observations
            .iter()
            .map(|observation| observation.candidate.timestamp),
    );
    timestamps.into_iter().max()
}

async fn load_history_heads(pool: &PgPool, logical_name_id: &str) -> Result<HistoryHeads> {
    let resource_ids = load_name_resource_ids(pool, logical_name_id).await?;
    let surface_head = load_name_history_head(
        pool,
        logical_name_id,
        &resource_ids,
        HistoryScope::Surface,
        true,
    )
    .await
    .with_context(|| {
        format!("failed to load surface history head for logical_name_id {logical_name_id}")
    })?;
    let resource_head = load_name_history_head(
        pool,
        logical_name_id,
        &resource_ids,
        HistoryScope::Resource,
        true,
    )
    .await
    .with_context(|| {
        format!("failed to load resource history head for logical_name_id {logical_name_id}")
    })?;

    Ok(HistoryHeads {
        surface_head,
        resource_head,
    })
}

async fn load_name_resource_ids(pool: &PgPool, logical_name_id: &str) -> Result<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .with_context(|| {
            format!("failed to load resource ids for logical_name_id {logical_name_id}")
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

async fn load_canonical_name_surfaces(pool: &PgPool) -> Result<Vec<NameSurfaceSeed>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY ns.logical_name_id
        "#
    ))
    .fetch_all(pool)
    .await
    .context("failed to load canonical name_surfaces for name_current rebuild")?;

    rows.into_iter().map(decode_name_surface_seed).collect()
}

async fn load_canonical_name_surface(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurfaceSeed>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.logical_name_id = $1
          AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical name_surface {logical_name_id} for name_current rebuild")
    })?;

    row.map(decode_name_surface_seed).transpose()
}

async fn load_current_binding_context(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<CurrentBindingContext>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id,
            sb.block_hash,
            sb.block_number,
            rb.block_timestamp,
            sb.canonicality_state::TEXT AS surface_binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = sb.chain_id
         AND rb.block_hash = sb.block_hash
        WHERE sb.logical_name_id = $1
          AND sb.active_to IS NULL
          AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY sb.active_from DESC, sb.surface_binding_id DESC
        LIMIT 1
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load current binding context for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_current_binding_context).transpose()
}

async fn load_wildcard_source_context(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
) -> Result<Option<WildcardSourceContext>> {
    if name.namespace != ENS_NAMESPACE
        || current_binding
            .is_none_or(|binding| binding.binding_kind != SurfaceBindingKind::ObservedWildcardPath)
    {
        return Ok(None);
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_to IS NULL
         AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        WHERE ns.namespace = $1
          AND ns.logical_name_id <> $2
          AND ns.canonicality_state {CANONICAL_STATE_FILTER}
          AND $3 LIKE ('%.' || ns.normalized_name)
        ORDER BY char_length(ns.normalized_name) DESC, sb.active_from DESC, sb.surface_binding_id DESC
        LIMIT 8
        "#
    ))
    .bind(&name.namespace)
    .bind(&name.logical_name_id)
    .bind(&name.normalized_name)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load wildcard source candidates for {}",
            name.logical_name_id
        )
    })?;

    for row in rows {
        let source_normalized_name = row
            .try_get::<String, _>("normalized_name")
            .context("missing wildcard source normalized_name")?;
        let Some(matched_labels) =
            wildcard_matched_labels(&name.normalized_name, &source_normalized_name)
        else {
            continue;
        };
        let logical_name_id = row
            .try_get::<String, _>("logical_name_id")
            .context("missing wildcard source logical_name_id")?;
        let resource_id = row
            .try_get::<Uuid, _>("resource_id")
            .context("missing wildcard source resource_id")?;
        let Some((resolver_event, boundary_event)) =
            load_wildcard_source_events(pool, &logical_name_id, resource_id).await?
        else {
            continue;
        };

        return Ok(Some(WildcardSourceContext {
            logical_name_id,
            namespace: row
                .try_get("namespace")
                .context("missing wildcard source namespace")?,
            normalized_name: source_normalized_name,
            canonical_display_name: row
                .try_get("canonical_display_name")
                .context("missing wildcard source canonical_display_name")?,
            namehash: row
                .try_get("namehash")
                .context("missing wildcard source namehash")?,
            resource_id,
            resolver_event,
            boundary_event,
            matched_labels,
        }));
    }

    Ok(None)
}

async fn load_wildcard_source_events(
    pool: &PgPool,
    logical_name_id: &str,
    resource_id: Uuid,
) -> Result<Option<(RelevantEvent, RelevantEvent)>> {
    let event_kinds = vec![
        EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
    ];
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            mv.manifest_version AS source_manifest_version,
            mv.namespace AS source_manifest_namespace,
            mv.source_family AS source_manifest_source_family,
            mv.chain AS source_manifest_chain,
            mv.deployment_epoch AS source_manifest_deployment_epoch,
            mv.rollout_status::TEXT AS source_manifest_rollout_status,
            mcf.status::TEXT AS exact_name_profile_status,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        LEFT JOIN manifest_versions mv
          ON mv.manifest_id = ne.source_manifest_id
        LEFT JOIN manifest_capability_flags mcf
          ON mcf.manifest_id = ne.source_manifest_id
         AND mcf.capability_name = 'exact_name_profile'
        WHERE ne.namespace = $1
          AND ne.logical_name_id = $2
          AND ne.resource_id = $3
          AND ne.event_kind = ANY($4::TEXT[])
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number DESC NULLS LAST,
            COALESCE(ne.log_index, -1) DESC,
            ne.normalized_event_id DESC
        LIMIT 16
        "#
    ))
    .bind(ENS_NAMESPACE)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(&event_kinds)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load wildcard source events for logical_name_id {logical_name_id}")
    })?;
    let events = rows
        .into_iter()
        .map(decode_relevant_event)
        .collect::<Result<Vec<_>>>()?;

    let resolver_event = events
        .iter()
        .find(|event| {
            event.event_kind == EVENT_KIND_RESOLVER_CHANGED
                && normalize_resolver_address(
                    json_str(&event.after_state, &["resolver"]).as_deref(),
                )
                .is_some()
                && event.chain_id.is_some()
        })
        .cloned();
    let boundary_event = events.iter().find(|event| {
        matches!(
            event.event_kind.as_str(),
            EVENT_KIND_RECORD_VERSION_CHANGED | EVENT_KIND_RESOLVER_CHANGED
        ) && relevant_event_chain_position(event).is_some()
    });

    Ok(resolver_event.zip(boundary_event.cloned()))
}

fn wildcard_matched_labels(requested_name: &str, source_name: &str) -> Option<Vec<String>> {
    let suffix = format!(".{source_name}");
    let prefix = requested_name.strip_suffix(&suffix)?;
    let labels = prefix.split('.').map(str::to_owned).collect::<Vec<_>>();
    (!labels.is_empty() && labels.iter().all(|label| !label.is_empty())).then_some(labels)
}

async fn load_relevant_events(pool: &PgPool, name: &NameSurfaceSeed) -> Result<Vec<RelevantEvent>> {
    let event_kinds = RELEVANT_EVENT_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    let derivation_kinds = vec![
        ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        ENS_V2_REGISTRY_DERIVATION_KIND.to_owned(),
        ENS_V2_REGISTRAR_DERIVATION_KIND.to_owned(),
        ENS_V2_RESOLVER_DERIVATION_KIND.to_owned(),
    ];
    let rows = if name.namespace == BASENAMES_NAMESPACE {
        let source_families = [
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR.to_owned(),
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        ];
        sqlx::query(&format!(
            r#"
            SELECT
                ne.normalized_event_id,
                ne.resource_id,
                ne.event_kind,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                mv.manifest_version AS source_manifest_version,
                mv.namespace AS source_manifest_namespace,
                mv.source_family AS source_manifest_source_family,
                mv.chain AS source_manifest_chain,
                mv.deployment_epoch AS source_manifest_deployment_epoch,
                mv.rollout_status::TEXT AS source_manifest_rollout_status,
                mcf.status::TEXT AS exact_name_profile_status,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                ne.after_state
            FROM normalized_events ne
            LEFT JOIN raw_blocks rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            LEFT JOIN manifest_versions mv
              ON mv.manifest_id = ne.source_manifest_id
            LEFT JOIN manifest_capability_flags mcf
              ON mcf.manifest_id = ne.source_manifest_id
             AND mcf.capability_name = 'exact_name_profile'
            WHERE ne.namespace = $1
              AND ne.logical_name_id = $2
              AND ne.derivation_kind = ANY($3::TEXT[])
              AND ne.event_kind = ANY($4::TEXT[])
              AND ne.source_family = ANY($5::TEXT[])
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.block_number NULLS FIRST,
                COALESCE(ne.log_index, 2147483647),
                ne.event_identity
            "#
        ))
        .bind(&name.namespace)
        .bind(&name.logical_name_id)
        .bind(&derivation_kinds)
        .bind(&event_kinds)
        .bind(&source_families)
        .fetch_all(pool)
        .await
    } else {
        sqlx::query(&format!(
            r#"
            SELECT
                ne.normalized_event_id,
                ne.resource_id,
                ne.event_kind,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                mv.manifest_version AS source_manifest_version,
                mv.namespace AS source_manifest_namespace,
                mv.source_family AS source_manifest_source_family,
                mv.chain AS source_manifest_chain,
                mv.deployment_epoch AS source_manifest_deployment_epoch,
                mv.rollout_status::TEXT AS source_manifest_rollout_status,
                mcf.status::TEXT AS exact_name_profile_status,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                ne.after_state
            FROM normalized_events ne
            LEFT JOIN raw_blocks rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            LEFT JOIN manifest_versions mv
              ON mv.manifest_id = ne.source_manifest_id
            LEFT JOIN manifest_capability_flags mcf
              ON mcf.manifest_id = ne.source_manifest_id
             AND mcf.capability_name = 'exact_name_profile'
            WHERE ne.namespace = $1
              AND ne.logical_name_id = $2
              AND ne.derivation_kind = ANY($3::TEXT[])
              AND ne.event_kind = ANY($4::TEXT[])
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.block_number NULLS FIRST,
                COALESCE(ne.log_index, 2147483647),
                ne.event_identity
            "#
        ))
        .bind(&name.namespace)
        .bind(&name.logical_name_id)
        .bind(&derivation_kinds)
        .bind(&event_kinds)
        .fetch_all(pool)
        .await
    }
    .with_context(|| {
        format!(
            "failed to load authority normalized events for {}",
            name.logical_name_id
        )
    })?;

    rows.into_iter().map(decode_relevant_event).collect()
}

fn build_exact_name_coverage(namespace: &str, events: &[RelevantEvent]) -> Value {
    if namespace == ENS_NAMESPACE {
        let has_ens_v2 = events.iter().any(|event| {
            matches!(
                event.source_family.as_str(),
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1 | SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
            )
        });
        let has_ens_v1 = events
            .iter()
            .any(|event| event.source_family.starts_with("ens_v1_"));
        if has_ens_v2 && has_ens_v1 {
            return json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": ens_v2_exact_name_coverage_source_classes(),
                "unsupported_reason": "mixed_ensv1_ensv2_exact_name_corpus",
                "enumeration_basis": "exact_name_profile",
            });
        }
        if has_ens_v2 && ens_v2_sepolia_dev_exact_name_supported(events) {
            return json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ens_v2_exact_name_coverage_source_classes(),
                "unsupported_reason": Value::Null,
                "enumeration_basis": "exact_name_profile",
            });
        }
        if has_ens_v2 {
            return json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": ["ensv2_registry_resource_surface"],
                "unsupported_reason": "ensv2_exact_name_profile_shadow",
                "enumeration_basis": "exact_name",
            });
        }
    }

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": exact_name_coverage_source_classes(namespace),
        "unsupported_reason": Value::Null,
        "enumeration_basis": "exact_name",
    })
}

fn ens_v2_sepolia_dev_exact_name_supported(events: &[RelevantEvent]) -> bool {
    let mut has_registry = false;
    let mut has_supported_registrar = false;

    for event in events
        .iter()
        .filter(|event| event.source_family.starts_with("ens_v2_"))
        .filter(|event| ens_v2_event_uses_active_selected_exact_name_manifest(event))
    {
        match event.source_family.as_str() {
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
                has_registry = true;
            }
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
                if event.exact_name_profile_status.as_deref()
                    == Some(CAPABILITY_STATUS_SUPPORTED) =>
            {
                has_supported_registrar = true;
            }
            _ => {}
        }
    }

    has_registry && has_supported_registrar
}

fn ens_v2_event_uses_active_selected_exact_name_manifest(event: &RelevantEvent) -> bool {
    event.source_manifest_id.is_some()
        && event.chain_id.as_deref() == Some(ETHEREUM_SEPOLIA_CHAIN_ID)
        && event.source_manifest_version == Some(event.manifest_version)
        && event.source_manifest_namespace.as_deref() == Some(ENS_NAMESPACE)
        && event.source_manifest_source_family.as_deref() == Some(event.source_family.as_str())
        && event.source_manifest_chain.as_deref() == Some(ETHEREUM_SEPOLIA_CHAIN_ID)
        && event.source_manifest_deployment_epoch.as_deref()
            == Some(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH)
        && event.source_manifest_rollout_status.as_deref() == Some(MANIFEST_ROLLOUT_STATUS_ACTIVE)
}

fn ens_v2_exact_name_coverage_source_classes() -> &'static [&'static str] {
    &[
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
    ]
}

fn exact_name_coverage_source_classes(namespace: &str) -> &'static [&'static str] {
    match namespace {
        ENS_NAMESPACE | BASENAMES_NAMESPACE => &["ensv1_registry_path"],
        _ => &[],
    }
}

fn decode_name_surface_seed(row: sqlx::postgres::PgRow) -> Result<NameSurfaceSeed> {
    Ok(NameSurfaceSeed {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing name_surface logical_name_id")?,
        namespace: row
            .try_get("namespace")
            .context("missing name_surface namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing name_surface canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing name_surface normalized_name")?,
        namehash: row
            .try_get("namehash")
            .context("missing name_surface namehash")?,
        chain_id: row
            .try_get("chain_id")
            .context("missing name_surface chain_id")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing name_surface block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing name_surface block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing raw_blocks.block_timestamp join for name_surface")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing name_surface canonicality_state")?,
        )?,
    })
}

fn decode_current_binding_context(row: sqlx::postgres::PgRow) -> Result<CurrentBindingContext> {
    Ok(CurrentBindingContext {
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id in current binding context")?,
        resource_id: row
            .try_get("resource_id")
            .context("missing resource_id in current binding context")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id in current binding context")?,
        binding_kind: parse_surface_binding_kind(
            &row.try_get::<String, _>("binding_kind")
                .context("missing binding_kind in current binding context")?,
        )?,
        chain_id: row
            .try_get("chain_id")
            .context("missing chain_id in current binding context")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing block_hash in current binding context")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number in current binding context")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp in current binding context")?,
        surface_binding_state: parse_canonicality_state(
            &row.try_get::<String, _>("surface_binding_state")
                .context("missing surface_binding_state in current binding context")?,
        )?,
        resource_state: parse_canonicality_state(
            &row.try_get::<String, _>("resource_state")
                .context("missing resource_state in current binding context")?,
        )?,
        token_lineage_state: row
            .try_get::<Option<String>, _>("token_lineage_state")
            .context("missing token_lineage_state in current binding context")?
            .map(|value| parse_canonicality_state(&value))
            .transpose()?,
    })
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        source_manifest_version: row
            .try_get("source_manifest_version")
            .context("missing source_manifest_version")?,
        source_manifest_namespace: row
            .try_get("source_manifest_namespace")
            .context("missing source_manifest_namespace")?,
        source_manifest_source_family: row
            .try_get("source_manifest_source_family")
            .context("missing source_manifest_source_family")?,
        source_manifest_chain: row
            .try_get("source_manifest_chain")
            .context("missing source_manifest_chain")?,
        source_manifest_deployment_epoch: row
            .try_get("source_manifest_deployment_epoch")
            .context("missing source_manifest_deployment_epoch")?,
        source_manifest_rollout_status: row
            .try_get("source_manifest_rollout_status")
            .context("missing source_manifest_rollout_status")?,
        exact_name_profile_status: row
            .try_get("exact_name_profile_status")
            .context("missing exact_name_profile_status")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        after_state: row.try_get("after_state").context("missing after_state")?,
    })
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state {value}"),
    }
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface_binding kind {value}"),
    }
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Observed => 0,
        CanonicalityState::Canonical => 1,
        CanonicalityState::Safe => 2,
        CanonicalityState::Finalized => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn weakest_canonicality(
    states: impl Iterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states.min_by_key(|state| canonicality_rank(*state))
}

fn chain_slot(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        _ => chain_id.to_owned(),
    }
}

fn format_timestamp(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(sqlx::types::time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
}

fn format_unix_timestamp_value(timestamp: Option<i64>) -> Value {
    match timestamp {
        Some(timestamp) => OffsetDateTime::from_unix_timestamp(timestamp)
            .map(format_timestamp)
            .map(Value::String)
            .unwrap_or_else(|_| Value::Number(timestamp.into())),
        None => Value::Null,
    }
}

fn json_str(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn json_i64(value: &Value, path: &[&str]) -> Option<i64> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_i64)
}

fn event_manifest_version(event: &RelevantEvent) -> Value {
    json!({
        "source_manifest_id": event.source_manifest_id,
        "source_family": event.source_family,
        "manifest_version": event.manifest_version,
    })
}

fn history_manifest_version(event: &HistoryEvent) -> Value {
    json!({
        "source_manifest_id": event.source_manifest_id,
        "source_family": event.source_family,
        "manifest_version": event.manifest_version,
    })
}

fn normalize_resolver_address(value: Option<&str>) -> Option<String> {
    let normalized = value?.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == ZERO_ADDRESS {
        None
    } else {
        Some(normalized)
    }
}

fn history_pointer_from_event(event: &HistoryEvent) -> HistoryPointer {
    HistoryPointer {
        normalized_event_id: event.normalized_event_id,
        event_kind: event.event_kind.clone(),
        chain_position: history_pointer_chain_position(event),
    }
}

fn history_pointer_chain_position(event: &HistoryEvent) -> Value {
    match (
        event.chain_id.as_ref(),
        event.block_number,
        event.block_hash.as_ref(),
        event.block_timestamp,
    ) {
        (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) => json!({
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(timestamp),
        }),
        _ => Value::Null,
    }
}

fn history_pointer_json(pointer: &HistoryPointer) -> Value {
    json!({
        "normalized_event_id": pointer.normalized_event_id,
        "event_kind": pointer.event_kind,
        "chain_position": pointer.chain_position,
    })
}

fn dedupe_json_values(values: impl IntoIterator<Item = Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON for dedupe")?;
        if seen.insert(key) {
            unique.push(value);
        }
    }

    Ok(unique)
}

#[cfg(test)]
mod tests;
