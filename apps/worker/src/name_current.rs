use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, HistoryEvent, HistoryScope, NameCurrentRow, SurfaceBindingKind,
    clear_name_current, delete_name_current, load_name_history_head,
    load_surface_bindings_by_logical_name_id, upsert_name_current_rows,
};
use serde_json::{Map, Value, json};
#[cfg(test)]
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
#[cfg(test)]
const SOURCE_FAMILY_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
const SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH: &str = "ens_v2_sepolia_dev";
#[cfg(test)]
const EXACT_NAME_PROFILE_CAPABILITY: &str = "exact_name_profile";
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
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use bigname_storage::{
        ChainLineageBlock, NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding,
        TokenLineage, default_database_url, load_name_current, upsert_chain_lineage_blocks,
        upsert_name_current_rows, upsert_name_surfaces, upsert_normalized_events,
        upsert_raw_blocks, upsert_resources, upsert_surface_bindings, upsert_token_lineages,
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
                .context("failed to parse database URL for worker name_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_name_current_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker name_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker name_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker name_current tests")?;

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

    #[tokio::test]
    async fn rebuilds_first_registration_into_name_current() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x1100, 0x2200, 0x3300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xname", 100, 1_717_171_700),
                raw_block("ethereum-mainnet", "0xgrant", 101, 1_717_171_701),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 101, 1_717_171_701).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-1",
                    "RegistrationGranted",
                    "0xgrant",
                    101,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "epoch-1",
                    "AuthorityEpochChanged",
                    "0xgrant",
                    101,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                ),
                authority_event(
                    &binding,
                    "bound-1",
                    "SurfaceBound",
                    "0xgrant",
                    101,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "active_from": 1_717_171_701_i64,
                        "binding_kind": "declared_registry_path",
                    }),
                ),
            ],
        )
        .await?;

        let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
        assert_eq!(summary.requested_name_count, 1);
        assert_eq!(summary.upserted_row_count, 1);

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::DeclaredRegistryPath)
        );
        assert_eq!(
            row.declared_summary["registration"]["status"],
            Value::String("active".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["authority_kind"],
            Value::String("registrar".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000aaa".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["expiry"],
            Value::String(format_timestamp(timestamp(1_800_000_000)))
        );
        assert_eq!(
            row.declared_summary["resolver"],
            json!({
                "chain_id": Value::Null,
                "address": Value::Null,
                "latest_event_kind": Value::Null,
            })
        );
        assert_eq!(
            row.declared_summary["record_inventory"]["status"],
            Value::String("unsupported".to_owned())
        );
        assert!(
            row.declared_summary["resolver"]
                .as_object()
                .and_then(|value| value.get("status"))
                .is_none()
        );
        assert!(
            row.declared_summary["history"]
                .as_object()
                .and_then(|value| value.get("status"))
                .is_none()
        );
        assert!(row.declared_summary["history"]["surface_head"].is_object());
        assert!(row.declared_summary["history"]["resource_head"].is_object());
        assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
        assert_eq!(row.coverage["unsupported_reason"], Value::Null);
        assert_eq!(row.manifest_version, 3);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_preserves_ens_v2_resource_identity_across_token_regeneration() -> Result<()> {
        let database = TestDatabase::new().await?;
        let (registry_manifest_id, registrar_manifest_id) =
            seed_ens_v2_exact_name_profile_manifests(database.pool()).await?;
        let binding =
            IdentityBinding::new("ens:bob.alice.eth", "bob.alice.eth", 0x9100, 0x9200, 0x9300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-sepolia", "0xensv2-surface", 700, 1_717_172_700),
                raw_block("ethereum-sepolia", "0xensv2-link", 701, 1_717_172_701),
                raw_block("ethereum-sepolia", "0xensv2-regen", 702, 1_717_172_702),
                raw_block("ethereum-sepolia", "0xensv2-renew", 703, 1_717_172_703),
            ],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id: binding.token_lineage_id,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-link".to_owned(),
                block_number: 701,
                provenance: json!({
                    "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    "current_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
                }),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[Resource {
                resource_id: binding.resource_id,
                token_lineage_id: Some(binding.token_lineage_id),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-link".to_owned(),
                block_number: 701,
                provenance: json!({
                    "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                    "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    "current_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
                }),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: "ens".to_owned(),
                input_name: binding.display_name.clone(),
                canonical_display_name: "Bob.alice.eth".to_owned(),
                normalized_name: binding.display_name.clone(),
                dns_encoded_name: binding.display_name.as_bytes().to_vec(),
                namehash: format!("namehash:{}", binding.display_name),
                labelhashes: vec![format!("labelhash:{}", binding.display_name)],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-surface".to_owned(),
                block_number: 700,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[SurfaceBinding {
                surface_binding_id: binding.surface_binding_id,
                logical_name_id: binding.logical_name_id.clone(),
                resource_id: binding.resource_id,
                binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
                active_from: timestamp(1_717_172_701),
                active_to: None,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-link".to_owned(),
                block_number: 701,
                provenance: json!({
                    "adapter": ENS_V2_REGISTRY_DERIVATION_KIND,
                    "binding_kind": "linked_subregistry_path",
                }),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                with_source_manifest_id(
                    ens_v2_registry_event(
                        &binding,
                        "token-resource",
                        "TokenResourceLinked",
                        "0xensv2-link",
                        701,
                        0,
                        json!({}),
                        json!({
                            "token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                            "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                            "resource_id": binding.resource_id.to_string(),
                        }),
                    ),
                    registry_manifest_id,
                ),
                with_source_manifest_id(
                    ens_v2_registry_event(
                        &binding,
                        "grant",
                        "RegistrationGranted",
                        "0xensv2-link",
                        701,
                        1,
                        json!({}),
                        json!({
                            "authority_kind": "ens_v2_registry",
                            "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                            "registrant": "0x0000000000000000000000000000000000000b0b",
                            "expiry": 1_900_000_000_i64,
                        }),
                    ),
                    registry_manifest_id,
                ),
                with_source_manifest_id(
                    ens_v2_registry_event(
                        &binding,
                        "regen",
                        "TokenRegenerated",
                        "0xensv2-regen",
                        702,
                        0,
                        json!({
                            "token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                        }),
                        json!({
                            "old_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a01",
                            "new_token_id": "0x0000000000000000000000000000000000000000000000000000000000000a02",
                            "resource_id": binding.resource_id.to_string(),
                        }),
                    ),
                    registry_manifest_id,
                ),
                with_source_manifest_id(
                    ens_v2_registrar_event(
                        &binding,
                        "renew",
                        "RegistrationRenewed",
                        "0xensv2-renew",
                        703,
                        0,
                        json!({}),
                        json!({
                            "duration": 31_536_000_i64,
                            "expiry": 1_931_536_000_i64,
                        }),
                    ),
                    registrar_manifest_id,
                ),
            ],
        )
        .await?;

        let summary = rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
        assert_eq!(summary.upserted_row_count, 1);
        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt ENSv2 row must exist")?;
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::LinkedSubregistryPath)
        );
        assert_eq!(
            row.declared_summary["registration"]["authority_kind"],
            Value::String("ens_v2_registry".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000b0b".to_owned())
        );
        assert!(
            row.provenance["normalized_event_ids"]
                .as_array()
                .is_some_and(|ids| ids.len() >= 4)
        );
        assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
        assert_eq!(
            row.coverage["exhaustiveness"],
            Value::String("authoritative".to_owned())
        );
        assert_eq!(
            row.coverage["source_classes_considered"],
            json!(["ens_v2_registry_l1", "ens_v2_registrar_l1"])
        );
        assert_eq!(row.coverage["unsupported_reason"], Value::Null);
        assert_eq!(
            row.coverage["enumeration_basis"],
            Value::String("exact_name_profile".to_owned())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_ignores_deprecated_ens_v2_registrar_shadow_events_after_supported_promotion()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let registry_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            1,
            MANIFEST_ROLLOUT_STATUS_ACTIVE,
        )
        .await?;
        let deprecated_registrar_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
            1,
            "deprecated",
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            deprecated_registrar_manifest_id,
            EXACT_NAME_PROFILE_CAPABILITY,
            "shadow",
        )
        .await?;
        let supported_registrar_manifest_id = insert_manifest_version(
            database.pool(),
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
            2,
            MANIFEST_ROLLOUT_STATUS_ACTIVE,
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            supported_registrar_manifest_id,
            EXACT_NAME_PROFILE_CAPABILITY,
            CAPABILITY_STATUS_SUPPORTED,
        )
        .await?;

        let binding = IdentityBinding::new(
            "ens:promotion.alice.eth",
            "promotion.alice.eth",
            0x9150,
            0x9250,
            0x9350,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(
                    ETHEREUM_SEPOLIA_CHAIN_ID,
                    "0xensv2-promotion-surface",
                    820,
                    1_717_172_820,
                ),
                raw_block(
                    ETHEREUM_SEPOLIA_CHAIN_ID,
                    "0xensv2-promotion-link",
                    821,
                    1_717_172_821,
                ),
                raw_block(
                    ETHEREUM_SEPOLIA_CHAIN_ID,
                    "0xensv2-promotion-deprecated",
                    822,
                    1_717_172_822,
                ),
                raw_block(
                    ETHEREUM_SEPOLIA_CHAIN_ID,
                    "0xensv2-promotion-supported",
                    823,
                    1_717_172_823,
                ),
            ],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id: binding.token_lineage_id,
                chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
                block_hash: "0xensv2-promotion-link".to_owned(),
                block_number: 821,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[Resource {
                resource_id: binding.resource_id,
                token_lineage_id: Some(binding.token_lineage_id),
                chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
                block_hash: "0xensv2-promotion-link".to_owned(),
                block_number: 821,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: ENS_NAMESPACE.to_owned(),
                input_name: binding.display_name.clone(),
                canonical_display_name: binding.display_name.clone(),
                normalized_name: binding.display_name.clone(),
                dns_encoded_name: binding.display_name.as_bytes().to_vec(),
                namehash: format!("namehash:{}", binding.display_name),
                labelhashes: vec![format!("labelhash:{}", binding.display_name)],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
                block_hash: "0xensv2-promotion-surface".to_owned(),
                block_number: 820,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[SurfaceBinding {
                surface_binding_id: binding.surface_binding_id,
                logical_name_id: binding.logical_name_id.clone(),
                resource_id: binding.resource_id,
                binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
                active_from: timestamp(1_717_172_821),
                active_to: None,
                chain_id: ETHEREUM_SEPOLIA_CHAIN_ID.to_owned(),
                block_hash: "0xensv2-promotion-link".to_owned(),
                block_number: 821,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;

        let mut deprecated_registrar_event = ens_v2_registrar_event(
            &binding,
            "deprecated-renew",
            "RegistrationRenewed",
            "0xensv2-promotion-deprecated",
            822,
            0,
            json!({}),
            json!({
                "duration": 31_536_000_i64,
                "expiry": 1_920_000_000_i64,
            }),
        );
        deprecated_registrar_event.manifest_version = 1;
        seed_events(
            database.pool(),
            &[
                with_source_manifest_id(
                    ens_v2_registry_event(
                        &binding,
                        "promotion-grant",
                        "RegistrationGranted",
                        "0xensv2-promotion-link",
                        821,
                        0,
                        json!({}),
                        json!({
                            "authority_kind": "ens_v2_registry",
                            "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                            "registrant": "0x0000000000000000000000000000000000000b0b",
                            "expiry": 1_900_000_000_i64,
                        }),
                    ),
                    registry_manifest_id,
                ),
                with_source_manifest_id(
                    deprecated_registrar_event,
                    deprecated_registrar_manifest_id,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
        let stale_only_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt stale ENSv2 row must exist")?;
        assert_eq!(
            stale_only_row.coverage["status"],
            Value::String("unsupported".to_owned())
        );
        assert_eq!(
            stale_only_row.coverage["unsupported_reason"],
            Value::String("ensv2_exact_name_profile_shadow".to_owned())
        );

        seed_events(
            database.pool(),
            &[with_source_manifest_id(
                ens_v2_registrar_event(
                    &binding,
                    "supported-renew",
                    "RegistrationRenewed",
                    "0xensv2-promotion-supported",
                    823,
                    0,
                    json!({}),
                    json!({
                        "duration": 31_536_000_i64,
                        "expiry": 1_931_536_000_i64,
                    }),
                ),
                supported_registrar_manifest_id,
            )],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;
        let promoted_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt promoted ENSv2 row must exist")?;
        assert_eq!(
            promoted_row.coverage["status"],
            Value::String("full".to_owned())
        );
        assert_eq!(promoted_row.coverage["unsupported_reason"], Value::Null);
        assert_eq!(
            promoted_row.coverage["enumeration_basis"],
            Value::String("exact_name_profile".to_owned())
        );
        assert_eq!(
            promoted_row.chain_positions["ethereum-sepolia"]["chain_id"],
            Value::String(ETHEREUM_SEPOLIA_CHAIN_ID.to_owned())
        );
        assert!(promoted_row.chain_positions.get("ethereum").is_none());

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_keeps_ens_v2_registry_only_exact_name_coverage_shadow() -> Result<()> {
        let database = TestDatabase::new().await?;
        let (registry_manifest_id, _) =
            seed_ens_v2_exact_name_profile_manifests(database.pool()).await?;
        let binding = IdentityBinding::new(
            "ens:registry-only.alice.eth",
            "registry-only.alice.eth",
            0x9140,
            0x9240,
            0x9340,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-registry-only-surface",
                    710,
                    1_717_172_710,
                ),
                raw_block(
                    "ethereum-sepolia",
                    "0xensv2-registry-only-link",
                    711,
                    1_717_172_711,
                ),
            ],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id: binding.token_lineage_id,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-registry-only-link".to_owned(),
                block_number: 711,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[Resource {
                resource_id: binding.resource_id,
                token_lineage_id: Some(binding.token_lineage_id),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-registry-only-link".to_owned(),
                block_number: 711,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: "ens".to_owned(),
                input_name: binding.display_name.clone(),
                canonical_display_name: binding.display_name.clone(),
                normalized_name: binding.display_name.clone(),
                dns_encoded_name: binding.display_name.as_bytes().to_vec(),
                namehash: format!("namehash:{}", binding.display_name),
                labelhashes: vec![format!("labelhash:{}", binding.display_name)],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-registry-only-surface".to_owned(),
                block_number: 710,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[SurfaceBinding {
                surface_binding_id: binding.surface_binding_id,
                logical_name_id: binding.logical_name_id.clone(),
                resource_id: binding.resource_id,
                binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
                active_from: timestamp(1_717_172_711),
                active_to: None,
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xensv2-registry-only-link".to_owned(),
                block_number: 711,
                provenance: json!({"adapter": ENS_V2_REGISTRY_DERIVATION_KIND}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        seed_events(
            database.pool(),
            &[with_source_manifest_id(
                ens_v2_registry_event(
                    &binding,
                    "registry-only-grant",
                    "RegistrationGranted",
                    "0xensv2-registry-only-link",
                    711,
                    0,
                    json!({}),
                    json!({
                        "authority_kind": "ens_v2_registry",
                        "authority_key": "ens-v2-registry:ethereum-sepolia:user-registry:0xeac",
                        "registrant": "0x0000000000000000000000000000000000000b0b",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                registry_manifest_id,
            )],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt registry-only ENSv2 row must exist")?;
        assert_eq!(
            row.coverage["status"],
            Value::String("unsupported".to_owned())
        );
        assert_eq!(
            row.coverage["exhaustiveness"],
            Value::String("not_applicable".to_owned())
        );
        assert_eq!(
            row.coverage["source_classes_considered"],
            json!(["ensv2_registry_resource_surface"])
        );
        assert_eq!(
            row.coverage["unsupported_reason"],
            Value::String("ensv2_exact_name_profile_shadow".to_owned())
        );

        database.cleanup().await
    }

    #[test]
    fn exact_name_coverage_rejects_mixed_ensv1_ensv2_corpus() {
        let coverage = build_exact_name_coverage(
            ENS_NAMESPACE,
            &[
                coverage_event("ens_v1_registrar_l1", "ethereum-mainnet"),
                coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, "ethereum-sepolia"),
                coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, "ethereum-sepolia"),
            ],
        );

        assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
        assert_eq!(
            coverage["unsupported_reason"],
            Value::String("mixed_ensv1_ensv2_exact_name_corpus".to_owned())
        );
        assert_eq!(
            coverage["source_classes_considered"],
            json!(["ens_v2_registry_l1", "ens_v2_registrar_l1"])
        );
        assert_eq!(
            coverage["enumeration_basis"],
            Value::String("exact_name_profile".to_owned())
        );
    }

    #[test]
    fn exact_name_coverage_rejects_ensv2_shadow_manifest_capability() {
        let coverage = build_exact_name_coverage(
            ENS_NAMESPACE,
            &[
                selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
                selected_ens_v2_coverage_event(
                    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
                    2,
                    101,
                    Some("shadow"),
                ),
            ],
        );

        assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
        assert_eq!(
            coverage["unsupported_reason"],
            Value::String("ensv2_exact_name_profile_shadow".to_owned())
        );
    }

    #[test]
    fn exact_name_coverage_rejects_ensv2_manifest_version_drift() {
        let mut drifted_registrar = selected_ens_v2_coverage_event(
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
            2,
            101,
            Some(CAPABILITY_STATUS_SUPPORTED),
        );
        drifted_registrar.source_manifest_version = Some(99);

        let coverage = build_exact_name_coverage(
            ENS_NAMESPACE,
            &[
                selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
                drifted_registrar,
            ],
        );

        assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
        assert_eq!(
            coverage["unsupported_reason"],
            Value::String("ensv2_exact_name_profile_shadow".to_owned())
        );
    }

    #[test]
    fn exact_name_coverage_rejects_ensv2_missing_manifest_linkage() {
        let mut unlinked_registrar =
            coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ETHEREUM_SEPOLIA_CHAIN_ID);
        unlinked_registrar.exact_name_profile_status = Some(CAPABILITY_STATUS_SUPPORTED.to_owned());

        let coverage = build_exact_name_coverage(
            ENS_NAMESPACE,
            &[
                selected_ens_v2_coverage_event(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, 1, 100, None),
                unlinked_registrar,
            ],
        );

        assert_eq!(coverage["status"], Value::String("unsupported".to_owned()));
        assert_eq!(
            coverage["unsupported_reason"],
            Value::String("ensv2_exact_name_profile_shadow".to_owned())
        );
    }

    #[tokio::test]
    async fn rebuild_projects_current_resolver_summary() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding =
            IdentityBinding::new("ens:resolver.eth", "resolver.eth", 0x3100, 0x3200, 0x3300);
        let resolver_address = "0x0000000000000000000000000000000000000abc";

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 210, 1_717_171_710),
                raw_block("ethereum-mainnet", "0xresolver", 211, 1_717_171_711),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 210, 1_717_171_710).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-resolver",
                    "RegistrationGranted",
                    "0xgrant",
                    210,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:resolver",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                resolver_event(
                    &binding,
                    "resolver-change",
                    resolver_address,
                    "0xresolver",
                    211,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(
            row.declared_summary["resolver"],
            json!({
                "chain_id": "ethereum-mainnet",
                "address": resolver_address,
                "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
            })
        );
        assert_eq!(row.coverage["unsupported_reason"], Value::Null);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_null_resolver_summary_for_zero_address() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new(
            "ens:no-resolver.eth",
            "no-resolver.eth",
            0x3400,
            0x3500,
            0x3600,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 220, 1_717_171_720),
                raw_block("ethereum-mainnet", "0xresolver", 221, 1_717_171_721),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 220, 1_717_171_720).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-null-resolver",
                    "RegistrationGranted",
                    "0xgrant",
                    220,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:no-resolver",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                resolver_event(
                    &binding,
                    "resolver-cleared",
                    ZERO_ADDRESS,
                    "0xresolver",
                    221,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(
            row.declared_summary["resolver"],
            json!({
                "chain_id": Value::Null,
                "address": Value::Null,
                "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_supported_alias_only_topology_from_alias_changed() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x3410, 0x3510, 0x3610);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xgrant", 230, 1_717_171_730),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 231, 1_717_171_731),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xalias", 232, 1_717_171_732),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbinding-alias",
                    233,
                    1_717_171_733,
                ),
            ],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[token_lineage(binding.token_lineage_id, "0xgrant", 230)],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[resource(
                binding.resource_id,
                binding.token_lineage_id,
                "0xgrant",
                230,
            )],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                &binding.logical_name_id,
                &binding.display_name,
                "0xgrant",
                230,
            )],
        )
        .await?;
        let alias_binding_id = Uuid::from_u128(0x3611);
        let mut alias_binding = surface_binding(
            &IdentityBinding {
                surface_binding_id: alias_binding_id,
                ..binding.clone()
            },
            1_717_171_733,
            None,
            "0xbinding-alias",
            233,
        );
        alias_binding.binding_kind = SurfaceBindingKind::ResolverAliasPath;
        upsert_surface_bindings(database.pool(), &[alias_binding]).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-alias",
                    "RegistrationGranted",
                    "0xgrant",
                    230,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                resolver_event(
                    &binding,
                    "alias-resolver",
                    "0x0000000000000000000000000000000000000abc",
                    "0xresolver",
                    231,
                    0,
                ),
                ens_v2_alias_event(
                    &binding,
                    "alias-project",
                    "0xalias",
                    232,
                    0,
                    json!({
                        "active": true,
                        "alias_state": "active",
                        "to_name": "profile.alice.eth",
                        "to_logical_name_id": "ens:profile.alice.eth",
                        "to_normalized_name": "profile.alice.eth",
                        "to_canonical_display_name": "Profile.alice.eth",
                        "to_namehash": "namehash:profile.alice.eth",
                        "to_resource_id": binding.resource_id.to_string(),
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt alias-only row must exist")?;
        let topology = row
            .declared_summary
            .get("topology")
            .context("supported alias-only row must project topology")?;
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::ResolverAliasPath)
        );
        assert_eq!(
            topology["alias"]["final_target"]["logical_name_id"],
            json!("ens:profile.alice.eth")
        );
        assert_eq!(
            topology["resolver_path"][0]["logical_name_id"],
            json!(binding.logical_name_id.clone())
        );
        assert_eq!(topology["wildcard"], empty_wildcard_detail());
        assert_eq!(topology["transport"], empty_transport_detail());
        assert_eq!(
            topology["version_boundaries"]["record_version_boundary"]["chain_position"]["block_hash"],
            json!("0xbinding-alias")
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_supported_wildcard_topology_from_real_ancestor_inputs() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x3420, 0x3520, 0x3620);
        let wildcard_source = IdentityBinding::new("ens:eth", "eth", 0x4420, 0x4520, 0x4620);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsource", 239, 1_717_171_739),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xgrant", 240, 1_717_171_740),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 241, 1_717_171_741),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbinding-wildcard",
                    242,
                    1_717_171_742,
                ),
            ],
        )
        .await?;
        seed_identity(
            database.pool(),
            &wildcard_source,
            "0xsource",
            239,
            1_717_171_739,
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[token_lineage(binding.token_lineage_id, "0xgrant", 240)],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[resource(
                binding.resource_id,
                binding.token_lineage_id,
                "0xgrant",
                240,
            )],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                &binding.logical_name_id,
                &binding.display_name,
                "0xgrant",
                240,
            )],
        )
        .await?;
        let wildcard_binding_id = Uuid::from_u128(0x3621);
        let mut wildcard_binding = surface_binding(
            &IdentityBinding {
                surface_binding_id: wildcard_binding_id,
                ..binding.clone()
            },
            1_717_171_742,
            None,
            "0xbinding-wildcard",
            242,
        );
        wildcard_binding.binding_kind = SurfaceBindingKind::ObservedWildcardPath;
        upsert_surface_bindings(database.pool(), &[wildcard_binding]).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-wildcard",
                    "RegistrationGranted",
                    "0xgrant",
                    240,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                resolver_event(
                    &wildcard_source,
                    "wildcard-resolver",
                    "0x0000000000000000000000000000000000000def",
                    "0xresolver",
                    241,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt wildcard-derived row must exist")?;
        let topology = row
            .declared_summary
            .get("topology")
            .context("supported wildcard-derived row must project topology")?;
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::ObservedWildcardPath)
        );
        assert_eq!(
            topology["resolver_path"][0]["logical_name_id"],
            json!("ens:eth")
        );
        assert_eq!(
            topology["resolver_path"][0]["resource_id"],
            json!(wildcard_source.resource_id.to_string())
        );
        assert_eq!(
            topology["resolver_path"][0]["address"],
            json!("0x0000000000000000000000000000000000000def")
        );
        assert_eq!(
            topology["wildcard"]["source"]["logical_name_id"],
            json!("ens:eth")
        );
        assert_eq!(
            topology["wildcard"]["source"]["resource_id"],
            json!(wildcard_source.resource_id.to_string())
        );
        assert_eq!(topology["wildcard"]["matched_labels"], json!(["alice"]));
        assert_eq!(topology["alias"], empty_alias_detail());
        assert_eq!(topology["transport"], empty_transport_detail());
        assert_eq!(
            topology["version_boundaries"]["record_version_boundary"]["logical_name_id"],
            json!("ens:eth")
        );
        assert_eq!(
            topology["version_boundaries"]["record_version_boundary"]["resource_id"],
            json!(wildcard_source.resource_id.to_string())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_basenames_base_authority_into_name_current() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new(
            "basenames:alice.base.eth",
            "alice.base.eth",
            0x4401,
            0x4402,
            0x4403,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xbase-surface", 500, 1_717_172_500),
                raw_block("base-mainnet", "0xbase-grant", 501, 1_717_172_501),
                raw_block("base-mainnet", "0xbase-transfer", 502, 1_717_172_502),
                raw_block("base-mainnet", "0xbase-resolver", 503, 1_717_172_503),
            ],
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            &binding,
            "0xbase-grant",
            501,
            1_717_172_501,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_authority_event(
                    &binding,
                    "base-grant",
                    "RegistrationGranted",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xbase-grant",
                    501,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                basenames_authority_event(
                    &binding,
                    "base-transfer",
                    "AuthorityTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                    "0xbase-transfer",
                    502,
                    Some(0),
                    json!({}),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000bbb",
                    }),
                ),
                basenames_resolver_event(
                    &binding,
                    "base-resolver",
                    "0x0000000000000000000000000000000000000abc",
                    "0xbase-resolver",
                    503,
                    0,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt basenames row must exist")?;
        assert_eq!(row.namespace, BASENAMES_NAMESPACE);
        assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.declared_summary["registration"]["status"],
            Value::String("active".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["authority_key"],
            Value::String("registrar:base-mainnet:alice".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["registry_owner"],
            Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
        );
        assert_eq!(
            row.declared_summary["resolver"],
            json!({
                "chain_id": "base-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": EVENT_KIND_RESOLVER_CHANGED,
            })
        );
        assert_eq!(
            row.coverage["source_classes_considered"],
            json!(["ensv1_registry_path"])
        );
        assert_eq!(
            row.chain_positions["base"]["chain_id"],
            Value::String("base-mainnet".to_owned())
        );
        assert!(row.declared_summary.get("topology").is_none());
        assert_eq!(row.coverage["unsupported_reason"], Value::Null);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_supported_basenames_transport_topology_from_frozen_inputs()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new(
            "basenames:alice.base.eth",
            "alice.base.eth",
            0x4411,
            0x4412,
            0x4413,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-surface", 500, 1_717_172_510),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-binding", 511, 1_717_172_511),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resolver", 512, 1_717_172_512),
                raw_block(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-record-version",
                    513,
                    1_717_172_513,
                ),
                raw_block(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-supported-binding",
                    514,
                    1_717_172_514,
                ),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1-future",
                    21_000_100,
                    1_776_387_700,
                ),
            ],
        )
        .await?;
        insert_chain_checkpoint(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xbasenamesl1-future",
            21_000_100,
        )
        .await?;
        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1-compatible",
                    21_000_099,
                    1_717_172_513,
                ),
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1-future",
                    21_000_100,
                    1_776_387_700,
                ),
            ],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id: binding.token_lineage_id,
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-binding".to_owned(),
                block_number: 511,
                provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[Resource {
                resource_id: binding.resource_id,
                token_lineage_id: Some(binding.token_lineage_id),
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-binding".to_owned(),
                block_number: 511,
                provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                input_name: binding.display_name.clone(),
                canonical_display_name: "Alice.base.eth".to_owned(),
                normalized_name: binding.display_name.clone(),
                dns_encoded_name: binding.display_name.as_bytes().to_vec(),
                namehash: format!("namehash:{}", binding.display_name),
                labelhashes: vec![format!("labelhash:{}", binding.display_name)],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 500,
                provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        let basenames_binding = SurfaceBinding {
            surface_binding_id: Uuid::from_u128(0x4414),
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_172_514),
            active_to: None,
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-supported-binding".to_owned(),
            block_number: 514,
            provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        };
        upsert_surface_bindings(database.pool(), &[basenames_binding.clone()]).await?;
        insert_basenames_execution_manifest_version(
            database.pool(),
            2,
            MANIFEST_ROLLOUT_STATUS_ACTIVE,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_authority_event(
                    &binding,
                    "supported-base-grant",
                    "RegistrationGranted",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xbase-binding",
                    511,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                basenames_resolver_event(
                    &binding,
                    "supported-base-resolver",
                    "0x0000000000000000000000000000000000000abc",
                    "0xbase-resolver",
                    512,
                    0,
                ),
                basenames_record_version_event(
                    &binding,
                    "supported-base-record-version",
                    "0xbase-record-version",
                    513,
                    0,
                    7,
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt supported basenames row must exist")?;
        let topology = row
            .declared_summary
            .get("topology")
            .context("supported basenames row must project topology")?;
        assert_eq!(
            topology["transport"],
            json!({
                "source_chain_id": BASE_MAINNET_CHAIN_ID,
                "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                "latest_event_kind": Value::Null,
            })
        );
        assert_eq!(
            row.chain_positions["base"]["chain_id"],
            json!(BASE_MAINNET_CHAIN_ID)
        );
        assert_eq!(
            row.chain_positions["ethereum"]["block_hash"],
            json!("0xbasenamesl1-compatible")
        );
        assert_eq!(
            row.chain_positions["ethereum"]["block_number"],
            json!(21_000_099)
        );
        assert_eq!(
            row.chain_positions["base"]["block_hash"],
            json!("0xbase-supported-binding")
        );
        assert_eq!(
            topology["version_boundaries"]["record_version_boundary"]["event_kind"],
            json!(EVENT_KIND_RECORD_VERSION_CHANGED)
        );
        assert_eq!(
            topology["version_boundaries"]["record_version_boundary"]["chain_position"]["block_hash"],
            json!("0xbase-record-version")
        );
        assert!(
            row.provenance["manifest_versions"]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| {
                    item.get("source_family").and_then(Value::as_str)
                        == Some(SOURCE_FAMILY_BASENAMES_EXECUTION)
                        && item.get("manifest_version").and_then(Value::as_i64) == Some(2)
                }))
        );
        assert_eq!(
            row.canonicality_summary["chains"][BASE_MAINNET_CHAIN_ID],
            json!("finalized")
        );
        assert_eq!(
            row.canonicality_summary["chains"][ETHEREUM_MAINNET_CHAIN_ID],
            json!("finalized")
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_projects_basenames_base_authority_control_vectors_into_name_current()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let nft_only = IdentityBinding::new(
            "basenames:nft-only.base.eth",
            "nft-only.base.eth",
            0x4411,
            0x4412,
            0x4413,
        );
        let management_only = IdentityBinding::new(
            "basenames:management-only.base.eth",
            "management-only.base.eth",
            0x4421,
            0x4422,
            0x4423,
        );
        let full_transfer = IdentityBinding::new(
            "basenames:full-transfer.base.eth",
            "full-transfer.base.eth",
            0x4431,
            0x4432,
            0x4433,
        );

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("base-mainnet", "0xbase-surface", 500, 1_717_182_500),
                raw_block("base-mainnet", "0xnft-grant", 501, 1_717_182_501),
                raw_block("base-mainnet", "0xnft-manager", 502, 1_717_182_502),
                raw_block("base-mainnet", "0xnft-transfer", 503, 1_717_182_503),
                raw_block("base-mainnet", "0xmgmt-grant", 511, 1_717_182_511),
                raw_block("base-mainnet", "0xmgmt-manager", 512, 1_717_182_512),
                raw_block("base-mainnet", "0xfull-grant", 521, 1_717_182_521),
                raw_block("base-mainnet", "0xfull-manager", 522, 1_717_182_522),
                raw_block("base-mainnet", "0xfull-transfer", 523, 1_717_182_523),
                raw_block("base-mainnet", "0xfull-manager-final", 524, 1_717_182_524),
            ],
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            &nft_only,
            "0xnft-grant",
            501,
            1_717_182_501,
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            &management_only,
            "0xmgmt-grant",
            511,
            1_717_182_511,
        )
        .await?;
        seed_basenames_identity(
            database.pool(),
            &full_transfer,
            "0xfull-grant",
            521,
            1_717_182_521,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                basenames_authority_event(
                    &nft_only,
                    "nft-grant",
                    "RegistrationGranted",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xnft-grant",
                    501,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:nft-only",
                        "registrant": "0x00000000000000000000000000000000000000a1",
                    }),
                ),
                basenames_authority_event(
                    &nft_only,
                    "nft-manager",
                    "AuthorityTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                    "0xnft-manager",
                    502,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a1",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b1",
                    }),
                ),
                basenames_authority_event(
                    &nft_only,
                    "nft-transfer",
                    "TokenControlTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xnft-transfer",
                    503,
                    Some(0),
                    json!({
                        "from": "0x00000000000000000000000000000000000000a1",
                    }),
                    json!({
                        "to": "0x00000000000000000000000000000000000000c1",
                    }),
                ),
                basenames_authority_event(
                    &management_only,
                    "mgmt-grant",
                    "RegistrationGranted",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xmgmt-grant",
                    511,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:management-only",
                        "registrant": "0x00000000000000000000000000000000000000a2",
                    }),
                ),
                basenames_authority_event(
                    &management_only,
                    "mgmt-manager",
                    "AuthorityTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                    "0xmgmt-manager",
                    512,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a2",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b2",
                    }),
                ),
                basenames_authority_event(
                    &full_transfer,
                    "full-grant",
                    "RegistrationGranted",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xfull-grant",
                    521,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:full-transfer",
                        "registrant": "0x00000000000000000000000000000000000000a3",
                    }),
                ),
                basenames_authority_event(
                    &full_transfer,
                    "full-manager",
                    "AuthorityTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                    "0xfull-manager",
                    522,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000a3",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                ),
                basenames_authority_event(
                    &full_transfer,
                    "full-transfer",
                    "TokenControlTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
                    "0xfull-transfer",
                    523,
                    Some(0),
                    json!({
                        "from": "0x00000000000000000000000000000000000000a3",
                    }),
                    json!({
                        "to": "0x00000000000000000000000000000000000000c3",
                    }),
                ),
                basenames_authority_event(
                    &full_transfer,
                    "full-manager-final",
                    "AuthorityTransferred",
                    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
                    "0xfull-manager-final",
                    524,
                    Some(0),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000b3",
                    }),
                    json!({
                        "owner": "0x00000000000000000000000000000000000000c3",
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), None).await?;

        let nft_only_row = load_name_current(database.pool(), &nft_only.logical_name_id)
            .await?
            .context("nft-only basenames row must exist")?;
        assert_eq!(nft_only_row.namespace, BASENAMES_NAMESPACE);
        assert_eq!(
            nft_only_row.declared_summary["control"]["registrant"],
            Value::String("0x00000000000000000000000000000000000000c1".to_owned())
        );
        assert_eq!(
            nft_only_row.declared_summary["control"]["registry_owner"],
            Value::String("0x00000000000000000000000000000000000000b1".to_owned())
        );
        assert_eq!(
            nft_only_row.declared_summary["control"]["latest_event_kind"],
            Value::String("TokenControlTransferred".to_owned())
        );

        let management_only_row =
            load_name_current(database.pool(), &management_only.logical_name_id)
                .await?
                .context("management-only basenames row must exist")?;
        assert_eq!(management_only_row.namespace, BASENAMES_NAMESPACE);
        assert_eq!(
            management_only_row.declared_summary["control"]["registrant"],
            Value::String("0x00000000000000000000000000000000000000a2".to_owned())
        );
        assert_eq!(
            management_only_row.declared_summary["control"]["registry_owner"],
            Value::String("0x00000000000000000000000000000000000000b2".to_owned())
        );
        assert_eq!(
            management_only_row.declared_summary["control"]["latest_event_kind"],
            Value::String("AuthorityTransferred".to_owned())
        );

        let full_transfer_row = load_name_current(database.pool(), &full_transfer.logical_name_id)
            .await?
            .context("full-transfer basenames row must exist")?;
        assert_eq!(full_transfer_row.namespace, BASENAMES_NAMESPACE);
        assert_eq!(
            full_transfer_row.declared_summary["control"]["registrant"],
            Value::String("0x00000000000000000000000000000000000000c3".to_owned())
        );
        assert_eq!(
            full_transfer_row.declared_summary["control"]["registry_owner"],
            Value::String("0x00000000000000000000000000000000000000c3".to_owned())
        );
        assert_eq!(
            full_transfer_row.declared_summary["control"]["latest_event_kind"],
            Value::String("AuthorityTransferred".to_owned())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_keeps_same_binding_for_renewal_and_transfer() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x4100, 0x4200, 0x4300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 201, 1_717_171_801),
                raw_block("ethereum-mainnet", "0xrenew", 202, 1_717_171_802),
                raw_block("ethereum-mainnet", "0xtransfer", 203, 1_717_171_803),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 201, 1_717_171_801).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-2",
                    "RegistrationGranted",
                    "0xgrant",
                    201,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "renew-2",
                    "RegistrationRenewed",
                    "0xrenew",
                    202,
                    Some(1),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "expiry-2",
                    "ExpiryChanged",
                    "0xrenew",
                    202,
                    Some(2),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "transfer-2",
                    "TokenControlTransferred",
                    "0xtransfer",
                    203,
                    Some(0),
                    json!({
                        "from": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "to": "0x0000000000000000000000000000000000000bbb",
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(binding.surface_binding_id));
        assert_eq!(row.resource_id, Some(binding.resource_id));
        assert_eq!(row.token_lineage_id, Some(binding.token_lineage_id));
        assert_eq!(
            row.declared_summary["registration"]["expiry"],
            Value::Number(1_900_000_000_i64.into())
        );
        assert_eq!(
            row.declared_summary["registration"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["registrant"],
            Value::String("0x0000000000000000000000000000000000000bbb".to_owned())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_switches_to_rebound_authority_epoch_binding() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x5100, 0x5200, 0x5300);
        let rebound = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x6100, 0x6200, 0x6300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 301, 1_717_171_901),
                raw_block("ethereum-mainnet", "0xrebind", 302, 1_717_171_902),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 301, 1_717_171_901).await?;
        seed_rebound_identity(
            database.pool(),
            &binding,
            &rebound,
            "0xrebind",
            302,
            1_717_171_902,
        )
        .await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-3",
                    "RegistrationGranted",
                    "0xgrant",
                    301,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "release-3",
                    "RegistrationReleased",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "released_at": 1_717_171_902_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "epoch-3",
                    "AuthorityEpochChanged",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:ethereum-mainnet:alice",
                        "status": "wrapped",
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "transfer-3",
                    "AuthorityTransferred",
                    "0xrebind",
                    302,
                    Some(0),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000aaa",
                    }),
                    json!({
                        "owner": "0x0000000000000000000000000000000000000ccc",
                    }),
                ),
                authority_event(
                    &binding,
                    "unbound-3",
                    "SurfaceUnbound",
                    "0xrebind",
                    302,
                    None,
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                    }),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "active_to": 1_717_171_902_i64,
                    }),
                ),
                authority_event(
                    &rebound,
                    "bound-3",
                    "SurfaceBound",
                    "0xrebind",
                    302,
                    None,
                    json!({}),
                    json!({
                        "authority_kind": "registry_only",
                        "authority_key": "registry:ethereum-mainnet:alice",
                        "active_from": 1_717_171_902_i64,
                        "binding_kind": "declared_registry_path",
                    }),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(row.surface_binding_id, Some(rebound.surface_binding_id));
        assert_eq!(row.resource_id, Some(rebound.resource_id));
        assert_eq!(row.token_lineage_id, Some(rebound.token_lineage_id));
        assert_eq!(
            row.declared_summary["registration"]["authority_kind"],
            Value::String("registry_only".to_owned())
        );
        assert_eq!(
            row.declared_summary["registration"]["status"],
            Value::String("released".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["registry_owner"],
            Value::String("0x0000000000000000000000000000000000000ccc".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["status"],
            Value::String("wrapped".to_owned())
        );
        assert_eq!(
            row.declared_summary["control"]["expiry"],
            Value::String(format_timestamp(timestamp(1_900_000_000)))
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_preserves_observed_wildcard_binding_kind() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding =
            IdentityBinding::new("ens:wildcard.eth", "wildcard.eth", 0x3301, 0x3302, 0x3303);

        seed_raw_blocks(
            database.pool(),
            &[raw_block("ethereum-mainnet", "0xwild", 241, 1_717_171_741)],
        )
        .await?;
        upsert_token_lineages(
            database.pool(),
            &[token_lineage(binding.token_lineage_id, "0xwild", 241)],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[resource(
                binding.resource_id,
                binding.token_lineage_id,
                "0xwild",
                241,
            )],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface(
                &binding.logical_name_id,
                &binding.display_name,
                "0xwild",
                241,
            )],
        )
        .await?;

        let mut wildcard_binding = surface_binding(&binding, 1_717_171_741, None, "0xwild", 241);
        wildcard_binding.binding_kind = SurfaceBindingKind::ObservedWildcardPath;
        upsert_surface_bindings(database.pool(), &[wildcard_binding]).await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        assert_eq!(
            row.binding_kind,
            Some(SurfaceBindingKind::ObservedWildcardPath)
        );
        assert_eq!(row.coverage["status"], Value::String("full".to_owned()));
        assert_eq!(row.coverage["unsupported_reason"], Value::Null);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_history_heads_match_canonical_name_history_ordering() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding =
            IdentityBinding::new("ens:history.eth", "history.eth", 0x8100, 0x8200, 0x8300);
        let historical_resource_id = Uuid::from_u128(0x8400);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 510, 1_717_172_110),
                raw_block("ethereum-mainnet", "0xsurface", 511, 1_717_172_111),
                raw_block("ethereum-mainnet", "0xresource", 512, 1_717_172_112),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 510, 1_717_172_110).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-history",
                    "RegistrationGranted",
                    "0xgrant",
                    510,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:history",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                history_event(
                    "surface-head",
                    Some(&binding.logical_name_id),
                    Some(historical_resource_id),
                    Some("ethereum-mainnet"),
                    Some(511),
                    Some("0xsurface"),
                    Some("0xtx511"),
                    Some(0),
                ),
                history_event(
                    "resource-head",
                    Some("ens:other.eth"),
                    Some(binding.resource_id),
                    Some("ethereum-mainnet"),
                    Some(512),
                    Some("0xresource"),
                    Some("0xtx512"),
                    Some(0),
                ),
            ],
        )
        .await?;

        rebuild_name_current(database.pool(), Some(&binding.logical_name_id)).await?;

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("rebuilt row must exist")?;
        let resource_ids =
            load_name_resource_ids(database.pool(), &binding.logical_name_id).await?;
        let expected_surface_head = load_name_history_head(
            database.pool(),
            &binding.logical_name_id,
            &resource_ids,
            HistoryScope::Surface,
            true,
        )
        .await?
        .context("surface head must exist")?;
        let expected_resource_head = load_name_history_head(
            database.pool(),
            &binding.logical_name_id,
            &resource_ids,
            HistoryScope::Resource,
            true,
        )
        .await?
        .context("resource head must exist")?;

        assert_eq!(
            row.declared_summary["history"]["surface_head"],
            history_pointer_json(&history_pointer_from_event(&expected_surface_head))
        );
        assert_eq!(
            row.declared_summary["history"]["resource_head"],
            history_pointer_json(&history_pointer_from_event(&expected_resource_head))
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rebuild_is_idempotent() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x7100, 0x7200, 0x7300);

        seed_raw_blocks(
            database.pool(),
            &[
                raw_block("ethereum-mainnet", "0xgrant", 401, 1_717_172_001),
                raw_block("ethereum-mainnet", "0xrenew", 402, 1_717_172_002),
            ],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 401, 1_717_172_001).await?;
        seed_events(
            database.pool(),
            &[
                authority_event(
                    &binding,
                    "grant-4",
                    "RegistrationGranted",
                    "0xgrant",
                    401,
                    Some(0),
                    json!({}),
                    json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:ethereum-mainnet:7:alice",
                        "registrant": "0x0000000000000000000000000000000000000aaa",
                        "expiry": 1_800_000_000_i64,
                    }),
                ),
                authority_event(
                    &binding,
                    "renew-4",
                    "RegistrationRenewed",
                    "0xrenew",
                    402,
                    Some(1),
                    json!({
                        "expiry": 1_800_000_000_i64,
                    }),
                    json!({
                        "expiry": 1_900_000_000_i64,
                    }),
                ),
            ],
        )
        .await?;

        let first = rebuild_name_current(database.pool(), None).await?;
        assert_eq!(first.upserted_row_count, 1);
        let first_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("first rebuild row must exist")?;

        let second = rebuild_name_current(database.pool(), None).await?;
        assert_eq!(second.upserted_row_count, 1);
        let second_row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("second rebuild row must exist")?;

        assert_eq!(first_row, second_row);

        database.cleanup().await
    }

    #[tokio::test]
    async fn keyed_rebuild_keeps_visible_row_when_projection_build_fails() -> Result<()> {
        let database = TestDatabase::new().await?;
        let binding = IdentityBinding::new("ens:alice.eth", "alice.eth", 0x8100, 0x8200, 0x8300);

        seed_raw_blocks(
            database.pool(),
            &[raw_block("ethereum-mainnet", "0xgrant", 401, 1_717_172_001)],
        )
        .await?;
        seed_identity(database.pool(), &binding, "0xgrant", 401, 1_717_172_001).await?;
        upsert_name_current_rows(
            database.pool(),
            &[NameCurrentRow {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: "ens".to_owned(),
                canonical_display_name: "alice.eth".to_owned(),
                normalized_name: "alice.eth".to_owned(),
                namehash: "node:alice.eth".to_owned(),
                surface_binding_id: None,
                resource_id: None,
                token_lineage_id: None,
                binding_kind: None,
                declared_summary: json!({"status": "stale"}),
                provenance: json!({"derivation_kind": NAME_CURRENT_DERIVATION_KIND}),
                coverage: json!({"status": "supported"}),
                chain_positions: json!({}),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {"ethereum-mainnet": "finalized"}
                }),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_717_172_001),
            }],
        )
        .await?;
        seed_events(
            database.pool(),
            &[NormalizedEvent {
                event_identity: "resolver-missing-chain".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: Some(binding.logical_name_id.clone()),
                resource_id: Some(binding.resource_id),
                event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                manifest_version: 1,
                source_manifest_id: None,
                chain_id: None,
                block_number: Some(402),
                block_hash: Some("0xresolver".to_owned()),
                transaction_hash: Some("0xtxresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "block_hash": "0xresolver",
                    "log_index": 0
                }),
                derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
                canonicality_state: CanonicalityState::Finalized,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000def"
                }),
            }],
        )
        .await?;

        let error = rebuild_name_current(database.pool(), Some(&binding.logical_name_id))
            .await
            .expect_err("rebuild should fail when a resolver event is missing chain_id");
        assert!(error.to_string().contains("ResolverChanged event"));

        let row = load_name_current(database.pool(), &binding.logical_name_id)
            .await?
            .context("stale visible row should still exist after failed rebuild")?;
        assert_eq!(row.declared_summary["status"], json!("stale"));

        database.cleanup().await
    }

    #[derive(Clone, Debug)]
    struct IdentityBinding {
        logical_name_id: String,
        display_name: String,
        token_lineage_id: Uuid,
        resource_id: Uuid,
        surface_binding_id: Uuid,
    }

    impl IdentityBinding {
        fn new(
            logical_name_id: &str,
            display_name: &str,
            token_lineage: u128,
            resource: u128,
            binding: u128,
        ) -> Self {
            Self {
                logical_name_id: logical_name_id.to_owned(),
                display_name: display_name.to_owned(),
                token_lineage_id: Uuid::from_u128(token_lineage),
                resource_id: Uuid::from_u128(resource),
                surface_binding_id: Uuid::from_u128(binding),
            }
        }
    }

    async fn seed_identity(
        pool: &PgPool,
        binding: &IdentityBinding,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> Result<()> {
        upsert_token_lineages(
            pool,
            &[token_lineage(
                binding.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_resources(
            pool,
            &[resource(
                binding.resource_id,
                binding.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_name_surfaces(
            pool,
            &[name_surface(
                &binding.logical_name_id,
                &binding.display_name,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[surface_binding(
                binding,
                block_timestamp,
                None,
                block_hash,
                block_number,
            )],
        )
        .await?;
        Ok(())
    }

    async fn seed_rebound_identity(
        pool: &PgPool,
        first: &IdentityBinding,
        rebound: &IdentityBinding,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> Result<()> {
        upsert_token_lineages(
            pool,
            &[token_lineage(
                rebound.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_resources(
            pool,
            &[resource(
                rebound.resource_id,
                rebound.token_lineage_id,
                block_hash,
                block_number,
            )],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[
                surface_binding(
                    first,
                    1_717_171_901,
                    Some(timestamp(block_timestamp)),
                    "0xgrant",
                    301,
                ),
                surface_binding(rebound, block_timestamp, None, block_hash, block_number),
            ],
        )
        .await?;
        Ok(())
    }

    async fn seed_basenames_identity(
        pool: &PgPool,
        binding: &IdentityBinding,
        block_hash: &str,
        block_number: i64,
        _block_timestamp: i64,
    ) -> Result<()> {
        upsert_token_lineages(
            pool,
            &[TokenLineage {
                token_lineage_id: binding.token_lineage_id,
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_resources(
            pool,
            &[Resource {
                resource_id: binding.resource_id,
                token_lineage_id: Some(binding.token_lineage_id),
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_name_surfaces(
            pool,
            &[NameSurface {
                logical_name_id: binding.logical_name_id.clone(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                input_name: binding.display_name.clone(),
                canonical_display_name: "Alice.base.eth".to_owned(),
                normalized_name: binding.display_name.clone(),
                dns_encoded_name: binding.display_name.as_bytes().to_vec(),
                namehash: format!("namehash:{}", binding.display_name),
                labelhashes: vec![format!("labelhash:{}", binding.display_name)],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 500,
                provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        upsert_surface_bindings(
            pool,
            &[SurfaceBinding {
                surface_binding_id: binding.surface_binding_id,
                logical_name_id: binding.logical_name_id.clone(),
                resource_id: binding.resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_172_501),
                active_to: None,
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
                canonicality_state: CanonicalityState::Finalized,
            }],
        )
        .await?;
        Ok(())
    }

    async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
        upsert_raw_blocks(pool, blocks).await?;
        Ok(())
    }

    async fn seed_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
        upsert_normalized_events(pool, events).await?;
        Ok(())
    }

    async fn insert_chain_checkpoint(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO chain_checkpoints (
                chain_id,
                finalized_block_hash,
                finalized_block_number
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id)
            DO UPDATE SET
                finalized_block_hash = EXCLUDED.finalized_block_hash,
                finalized_block_number = EXCLUDED.finalized_block_number
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(block_number)
        .execute(pool)
        .await
        .with_context(|| format!("failed to insert chain checkpoint for {chain_id}"))?;

        Ok(())
    }

    async fn seed_ens_v2_exact_name_profile_manifests(pool: &PgPool) -> Result<(i64, i64)> {
        let registry_manifest_id = insert_manifest_version(
            pool,
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            1,
            MANIFEST_ROLLOUT_STATUS_ACTIVE,
        )
        .await?;
        let registrar_manifest_id = insert_manifest_version(
            pool,
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
            2,
            MANIFEST_ROLLOUT_STATUS_ACTIVE,
        )
        .await?;
        insert_capability_flag(
            pool,
            registrar_manifest_id,
            EXACT_NAME_PROFILE_CAPABILITY,
            CAPABILITY_STATUS_SUPPORTED,
        )
        .await?;

        Ok((registry_manifest_id, registrar_manifest_id))
    }

    async fn insert_manifest_version(
        pool: &PgPool,
        source_family: &str,
        manifest_version: i64,
        rollout_status: &str,
    ) -> Result<i64> {
        sqlx::query(
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
        .bind(ENS_NAMESPACE)
        .bind(source_family)
        .bind(ETHEREUM_SEPOLIA_CHAIN_ID)
        .bind(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH)
        .bind(rollout_status)
        .bind("ensip15@2026-04-16")
        .bind(format!(
            "tests/{source_family}/ens-v2-sepolia-dev-v{manifest_version}.toml"
        ))
        .bind(json!({}))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
        .try_get("manifest_id")
        .context("failed to read manifest_id")
    }

    async fn insert_basenames_execution_manifest_version(
        pool: &PgPool,
        manifest_version: i64,
        rollout_status: &str,
    ) -> Result<i64> {
        let manifest_id = sqlx::query(
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
        .bind(BASENAMES_NAMESPACE)
        .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
        .bind(ETHEREUM_MAINNET_CHAIN_ID)
        .bind(BASENAMES_V1_DEPLOYMENT_EPOCH)
        .bind(rollout_status)
        .bind("ensip15@2026-04-16")
        .bind(format!(
            "tests/{}/{}-v{manifest_version}.toml",
            SOURCE_FAMILY_BASENAMES_EXECUTION, BASENAMES_V1_DEPLOYMENT_EPOCH
        ))
        .bind(json!({}))
        .fetch_one(pool)
        .await
        .context("failed to insert basenames_execution manifest_version")?
        .try_get("manifest_id")
        .context("failed to read basenames_execution manifest_id")?;
        insert_capability_flag(
            pool,
            manifest_id,
            VERIFIED_RESOLUTION_CAPABILITY,
            CAPABILITY_STATUS_SUPPORTED,
        )
        .await?;
        insert_basenames_execution_manifest_contract(pool, manifest_id, manifest_version).await?;
        Ok(manifest_id)
    }

    async fn insert_basenames_execution_manifest_contract(
        pool: &PgPool,
        manifest_id: i64,
        manifest_version: i64,
    ) -> Result<()> {
        let contract_instance_id =
            Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0000 + manifest_version as u128);
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, 'contract', $3::jsonb)
            ON CONFLICT (contract_instance_id) DO NOTHING
            "#,
        )
        .bind(contract_instance_id)
        .bind(ETHEREUM_MAINNET_CHAIN_ID)
        .bind(json!({"seed": "worker_name_current_basenames_execution"}))
        .execute(pool)
        .await
        .context("failed to insert basenames_execution contract_instance")?;

        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES ($1, 'contract', 'l1_resolver', $2, $3, 'l1_resolver', 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .bind(BASENAMES_L1_RESOLVER_ADDRESS.to_ascii_lowercase())
        .execute(pool)
        .await
        .context("failed to insert basenames_execution manifest_contract_instance")?;

        Ok(())
    }

    async fn insert_capability_flag(
        pool: &PgPool,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO manifest_capability_flags (
                manifest_id,
                capability_name,
                status,
                notes
            )
            VALUES ($1, $2, $3::capability_support_status, NULL)
            "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .execute(pool)
        .await
        .with_context(|| format!("failed to insert capability flag {capability_name}"))?;

        Ok(())
    }

    fn raw_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        unix_timestamp: i64,
    ) -> RawBlock {
        RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(unix_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn chain_lineage_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        unix_timestamp: i64,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(format!("0xparent{block_number:08x}")),
            block_number,
            block_timestamp: timestamp(unix_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn token_lineage(token_lineage_id: Uuid, block_hash: &str, block_number: i64) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource(
        resource_id: Uuid,
        token_lineage_id: Uuid,
        block_hash: &str,
        block_number: i64,
    ) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface(
        logical_name_id: &str,
        display_name: &str,
        block_hash: &str,
        block_number: i64,
    ) -> NameSurface {
        NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "ens".to_owned(),
            input_name: display_name.to_owned(),
            canonical_display_name: display_name.to_owned(),
            normalized_name: display_name.to_owned(),
            dns_encoded_name: display_name.as_bytes().to_vec(),
            namehash: format!("namehash:{display_name}"),
            labelhashes: vec![format!("labelhash:{display_name}")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "name_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding(
        binding: &IdentityBinding,
        active_from_unix: i64,
        active_to: Option<OffsetDateTime>,
        block_hash: &str,
        block_number: i64,
    ) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id: binding.surface_binding_id,
            logical_name_id: binding.logical_name_id.clone(),
            resource_id: binding.resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(active_from_unix),
            active_to,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            provenance: json!({"source": "worker_name_current_test", "kind": "surface_binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn authority_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:{event_kind}:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v1_registrar_l1".to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state,
            after_state,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn ens_v2_registry_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:ens-v2:{event_kind}:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:ens-v2:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-sepolia",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:ens-v2:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V2_REGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state,
            after_state,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn ens_v2_registrar_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:ens-v2-registrar:{event_kind}:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned(),
            manifest_version: 2,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:ens-v2-registrar:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-sepolia",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:ens-v2-registrar:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V2_REGISTRAR_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state,
            after_state,
        }
    }

    fn ens_v2_alias_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!(
                "worker-test:ens-v2:{EVENT_KIND_ALIAS_CHANGED}:{identity_suffix}"
            ),
            namespace: ENS_NAMESPACE.to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: EVENT_KIND_ALIAS_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_ENS_V2_RESOLVER.to_owned(),
            manifest_version: 5,
            source_manifest_id: None,
            chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:ens-v2-alias:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:ens-v2-alias:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V2_RESOLVER_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state,
        }
    }

    fn with_source_manifest_id(
        mut event: NormalizedEvent,
        source_manifest_id: i64,
    ) -> NormalizedEvent {
        event.source_manifest_id = Some(source_manifest_id);
        event
    }

    fn coverage_event(source_family: &str, chain_id: &str) -> RelevantEvent {
        RelevantEvent {
            normalized_event_id: 1,
            resource_id: None,
            event_kind: "RegistrationGranted".to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            source_manifest_version: None,
            source_manifest_namespace: None,
            source_manifest_source_family: None,
            source_manifest_chain: None,
            source_manifest_deployment_epoch: None,
            source_manifest_rollout_status: None,
            exact_name_profile_status: None,
            chain_id: Some(chain_id.to_owned()),
            block_number: Some(1),
            block_hash: Some(format!("0x{chain_id}")),
            block_timestamp: None,
            raw_fact_ref: json!({"kind": "raw_log"}),
            canonicality_state: CanonicalityState::Finalized,
            after_state: json!({}),
        }
    }

    fn selected_ens_v2_coverage_event(
        source_family: &str,
        manifest_version: i64,
        source_manifest_id: i64,
        exact_name_profile_status: Option<&str>,
    ) -> RelevantEvent {
        let mut event = coverage_event(source_family, ETHEREUM_SEPOLIA_CHAIN_ID);
        event.manifest_version = manifest_version;
        event.source_manifest_id = Some(source_manifest_id);
        event.source_manifest_version = Some(manifest_version);
        event.source_manifest_namespace = Some(ENS_NAMESPACE.to_owned());
        event.source_manifest_source_family = Some(source_family.to_owned());
        event.source_manifest_chain = Some(ETHEREUM_SEPOLIA_CHAIN_ID.to_owned());
        event.source_manifest_deployment_epoch =
            Some(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH.to_owned());
        event.source_manifest_rollout_status = Some(MANIFEST_ROLLOUT_STATUS_ACTIVE.to_owned());
        event.exact_name_profile_status = exact_name_profile_status.map(str::to_owned);
        event
    }

    fn resolver_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        resolver_address: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:{EVENT_KIND_RESOLVER_CHANGED}:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: "ens_v1_unwrapped_authority".to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": resolver_address,
                "namehash": format!("namehash:{}", binding.display_name),
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn basenames_authority_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        event_kind: &str,
        source_family: &str,
        block_hash: &str,
        block_number: i64,
        log_index: Option<i64>,
        before_state: Value,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:{event_kind}:{identity_suffix}"),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: event_kind.to_owned(),
            source_family: source_family.to_owned(),
            manifest_version: 3,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state,
            after_state,
        }
    }

    fn basenames_resolver_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        resolver_address: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:{EVENT_KIND_RESOLVER_CHANGED}:{identity_suffix}"),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some("base-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "base-mainnet",
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "resolver": resolver_address,
                "namehash": format!("namehash:{}", binding.display_name),
            }),
        }
    }

    fn basenames_record_version_event(
        binding: &IdentityBinding,
        identity_suffix: &str,
        block_hash: &str,
        block_number: i64,
        log_index: i64,
        record_version: i64,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!(
                "worker-test:{EVENT_KIND_RECORD_VERSION_CHANGED}:{identity_suffix}"
            ),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            logical_name_id: Some(binding.logical_name_id.clone()),
            resource_id: Some(binding.resource_id),
            event_kind: EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
            manifest_version: 4,
            source_manifest_id: None,
            chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
            block_number: Some(block_number),
            block_hash: Some(block_hash.to_owned()),
            transaction_hash: Some(format!("tx:{identity_suffix}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_hash": block_hash,
                "block_number": block_number,
                "transaction_hash": format!("tx:{identity_suffix}"),
                "log_index": log_index,
            }),
            derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({
                "record_version": record_version - 1,
            }),
            after_state: json!({
                "record_version": record_version,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn history_event(
        identity_suffix: &str,
        logical_name_id: Option<&str>,
        resource_id: Option<Uuid>,
        chain_id: Option<&str>,
        block_number: Option<i64>,
        block_hash: Option<&str>,
        transaction_hash: Option<&str>,
        log_index: Option<i64>,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("worker-test:history:{identity_suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: logical_name_id.map(str::to_owned),
            resource_id,
            event_kind: "HistoryEvent".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 5,
            source_manifest_id: None,
            chain_id: chain_id.map(str::to_owned),
            block_number,
            block_hash: block_hash.map(str::to_owned),
            transaction_hash: transaction_hash.map(str::to_owned),
            log_index,
            raw_fact_ref: json!({
                "kind": "raw_log",
                "event_identity": identity_suffix,
                "transaction_hash": transaction_hash,
            }),
            derivation_kind: "history_test".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({}),
        }
    }

    fn timestamp(value: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
    }
}
