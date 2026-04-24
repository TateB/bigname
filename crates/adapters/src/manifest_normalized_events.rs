use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use bigname_manifests::{
    ManifestCodeHashObservation, ManifestDeclaredContractDriftInput, ManifestDriftActiveManifest,
    ManifestDriftInputs, ManifestProxyImplementationDriftEdge, load_manifest_drift_inputs,
};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Uuid};

const DERIVATION_KIND_MANIFEST_SYNC: &str = "manifest_sync";
const DERIVATION_KIND_MANIFEST_ALERT: &str = "manifest_alert";
const EVENT_KIND_SOURCE_MANIFEST_UPDATED: &str = "SourceManifestUpdated";
const EVENT_KIND_CAPABILITY_CHANGED: &str = "CapabilityChanged";
const EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED: &str = "ProxyImplementationChanged";
const EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT: &str = "ManifestCodeHashDriftAlert";
const EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT: &str = "ManifestProxyImplementationAlert";

/// Sync summary for normalized events derived from stored active manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventSyncSummary {
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, ManifestNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
struct ActiveCapabilityRow {
    capability_name: String,
    status: String,
    notes: Option<String>,
}

/// Sync manifest-derived normalized events from stored active manifest state.
pub async fn sync_manifest_normalized_events(
    pool: &PgPool,
) -> Result<ManifestNormalizedEventSyncSummary> {
    let drift_inputs = load_manifest_drift_inputs(pool).await?;
    if drift_inputs.active_manifests.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let capabilities = load_active_capabilities(pool).await?;
    let contracts = active_proxy_contracts_by_manifest(&drift_inputs);
    let before_counts = load_normalized_event_counts_by_kind(pool).await?;
    let events = build_normalized_events(&drift_inputs, &capabilities, &contracts)?;

    if events.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let synced_by_kind = count_events_by_kind(&events);
    upsert_normalized_events(pool, &events).await?;
    let after_counts = load_normalized_event_counts_by_kind(pool).await?;

    let mut by_kind = BTreeMap::new();
    let mut total_inserted_count = 0;
    for (kind, synced_count) in synced_by_kind {
        let inserted_count = after_counts
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .saturating_sub(before_counts.get(&kind).copied().unwrap_or(0));
        total_inserted_count += inserted_count;
        by_kind.insert(
            kind,
            ManifestNormalizedEventKindSyncSummary {
                synced_count,
                inserted_count,
            },
        );
    }

    Ok(ManifestNormalizedEventSyncSummary {
        total_synced_count: events.len(),
        total_inserted_count,
        by_kind,
    })
}

fn build_normalized_events(
    drift_inputs: &ManifestDriftInputs,
    capabilities: &HashMap<i64, Vec<ActiveCapabilityRow>>,
    contracts: &HashMap<i64, Vec<ManifestDeclaredContractDriftInput>>,
) -> Result<Vec<NormalizedEvent>> {
    let mut events = Vec::new();

    for manifest in &drift_inputs.active_manifests {
        events.push(build_source_manifest_updated_event(manifest)?);

        if let Some(capability_rows) = capabilities.get(&manifest.manifest_id) {
            for capability in capability_rows {
                events.push(build_capability_changed_event(manifest, capability)?);
            }
        }

        if let Some(contract_rows) = contracts.get(&manifest.manifest_id) {
            for contract in contract_rows {
                events.push(build_proxy_implementation_changed_event(
                    manifest, contract,
                )?);
            }
        }
    }

    events.extend(build_code_hash_drift_alert_events(drift_inputs)?);
    for edge in &drift_inputs.proxy_implementation_edges {
        events.push(build_proxy_implementation_alert_event(edge)?);
    }

    Ok(events)
}

fn build_source_manifest_updated_event(
    manifest: &ManifestDriftActiveManifest,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let deployment_epoch = manifest.deployment_epoch.clone();
    let normalizer_version = manifest.normalizer_version.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:source_manifest_updated",
            json!([
                manifest.manifest_id,
                manifest.manifest_version,
                namespace.clone(),
                source_family.clone(),
                chain.clone(),
                deployment_epoch.clone(),
                normalizer_version.clone(),
            ]),
        )?,
        namespace: namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
        source_family: source_family.clone(),
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "namespace": namespace.clone(),
            "source_family": source_family.clone(),
            "chain": chain.clone(),
            "deployment_epoch": deployment_epoch.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "manifest_version": manifest.manifest_version,
            "normalizer_version": normalizer_version,
        }),
    })
}

fn build_capability_changed_event(
    manifest: &ManifestDriftActiveManifest,
    capability: &ActiveCapabilityRow,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let capability_name = capability.capability_name.clone();
    let status = capability.status.clone();
    let notes = capability.notes.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:capability_changed",
            json!([
                manifest.manifest_id,
                capability_name.clone(),
                status.clone(),
                notes.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "capability_name": capability_name.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "capability_name": capability_name,
            "status": status,
            "notes": notes,
        }),
    })
}

fn build_proxy_implementation_changed_event(
    manifest: &ManifestDriftActiveManifest,
    contract: &ManifestDeclaredContractDriftInput,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let role = contract
        .role
        .clone()
        .unwrap_or_else(|| contract.declaration_name.clone());
    let address = contract.declared_address.clone();
    let proxy_kind = contract.proxy_kind.clone().unwrap_or_default();
    let implementation = contract
        .declared_implementation_address
        .clone()
        .unwrap_or_default();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:proxy_implementation_changed",
            json!([
                manifest.manifest_id,
                role.clone(),
                address.clone(),
                proxy_kind.clone(),
                implementation.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "role": role.clone(),
            "address": address.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "role": role,
            "address": address,
            "proxy_kind": proxy_kind,
            "implementation": implementation,
        }),
    })
}

fn build_code_hash_drift_alert_events(
    drift_inputs: &ManifestDriftInputs,
) -> Result<Vec<NormalizedEvent>> {
    let observations = drift_inputs
        .code_hash_observations
        .iter()
        .map(|observation| {
            (
                code_hash_observation_key(
                    &observation.chain,
                    observation.contract_instance_id,
                    &observation.address,
                ),
                observation,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    for declared_contract in &drift_inputs.declared_contracts {
        let Some(expected_code_hash) = declared_contract.code_hash.as_ref() else {
            continue;
        };
        let Some(observation) = observations.get(&code_hash_observation_key(
            &declared_contract.chain,
            declared_contract.contract_instance_id,
            &declared_contract.declared_address,
        )) else {
            continue;
        };
        if expected_code_hash.eq_ignore_ascii_case(&observation.code_hash) {
            continue;
        }
        events.push(build_code_hash_drift_alert_event(
            declared_contract,
            observation,
            expected_code_hash,
        )?);
    }

    Ok(events)
}

fn build_code_hash_drift_alert_event(
    declared_contract: &ManifestDeclaredContractDriftInput,
    observation: &ManifestCodeHashObservation,
    expected_code_hash: &str,
) -> Result<NormalizedEvent> {
    let canonicality_state = canonicality_state_from_view(&observation.canonicality_state)?;
    let contract_instance_id = declared_contract.contract_instance_id.to_string();
    let source_manifest_id = declared_contract.manifest_id;
    let namespace = declared_contract.namespace.clone();
    let source_family = declared_contract.source_family.clone();
    let chain = declared_contract.chain.clone();
    let address = declared_contract.declared_address.clone();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:code_hash_drift",
            json!([
                source_manifest_id,
                declared_contract.declaration_kind,
                declared_contract.declaration_name,
                contract_instance_id,
                address,
                expected_code_hash,
                observation.code_hash,
                observation.block_hash,
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(declared_contract.manifest_version)?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.clone()),
        block_number: Some(observation.block_number),
        block_hash: Some(observation.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": source_manifest_id,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": address,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_code_hash_drift",
            "alert_status": "active",
            "chain": chain,
            "source_family": declared_contract.source_family,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": declared_contract.declared_address,
            "expected_code_hash": expected_code_hash,
            "observed_code_hash": observation.code_hash,
            "observed_code_byte_length": observation.code_byte_length,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
            "observed_canonicality_state": observation.canonicality_state,
            "watched_source": watched_contract_source_name(observation),
            "source_manifest_id": observation.source_manifest_id,
        }),
    })
}

fn build_proxy_implementation_alert_event(
    edge: &ManifestProxyImplementationDriftEdge,
) -> Result<NormalizedEvent> {
    let proxy_contract_instance_id = edge.proxy_contract_instance_id.to_string();
    let implementation_contract_instance_id = edge.implementation_contract_instance_id.to_string();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:proxy_implementation",
            json!([
                edge.source_manifest_id,
                edge.discovery_edge_id,
                proxy_contract_instance_id,
                edge.proxy_address,
                implementation_contract_instance_id,
                edge.implementation_address,
            ]),
        )?,
        namespace: edge.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT.to_owned(),
        source_family: edge.source_family.clone(),
        manifest_version: manifest_version_i64(edge.manifest_version)?,
        source_manifest_id: Some(edge.source_manifest_id),
        chain_id: Some(edge.chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": edge.source_manifest_id,
            "discovery_edge_id": edge.discovery_edge_id,
            "proxy_contract_instance_id": proxy_contract_instance_id,
            "implementation_contract_instance_id": implementation_contract_instance_id,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_proxy_implementation_edge",
            "alert_status": "active",
            "chain": edge.chain,
            "source_family": edge.source_family,
            "proxy_contract_instance_id": edge.proxy_contract_instance_id.to_string(),
            "proxy_address": edge.proxy_address,
            "implementation_contract_instance_id": edge.implementation_contract_instance_id.to_string(),
            "implementation_address": edge.implementation_address,
            "declaration_name": edge.declaration_name,
            "role": edge.role,
            "proxy_kind": edge.proxy_kind,
            "admission": edge.admission,
            "active_from_block_number": edge.active_from_block_number,
            "active_to_block_number": edge.active_to_block_number,
            "provenance": edge.provenance,
        }),
    })
}

fn event_identity(prefix: &str, key: Value) -> Result<String> {
    Ok(format!(
        "{prefix}:{}",
        serde_json::to_string(&key).context("failed to serialize normalized-event identity")?
    ))
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

async fn load_active_capabilities(pool: &PgPool) -> Result<HashMap<i64, Vec<ActiveCapabilityRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::text AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active capability flags for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveCapabilityRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing capability manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveCapabilityRow {
                capability_name: row
                    .try_get("capability_name")
                    .context("missing capability_name")?,
                status: row.try_get("status").context("missing status")?,
                notes: row.try_get("notes").context("missing notes")?,
            });
    }

    Ok(grouped)
}

fn active_proxy_contracts_by_manifest(
    drift_inputs: &ManifestDriftInputs,
) -> HashMap<i64, Vec<ManifestDeclaredContractDriftInput>> {
    let mut grouped = HashMap::<i64, Vec<ManifestDeclaredContractDriftInput>>::new();
    for contract in &drift_inputs.declared_contracts {
        if contract.declaration_kind == "contract"
            && contract.implementation_contract_instance_id.is_some()
            && contract.declared_implementation_address.is_some()
        {
            grouped
                .entry(contract.manifest_id)
                .or_default()
                .push(contract.clone());
        }
    }
    for rows in grouped.values_mut() {
        rows.sort_by(|left, right| {
            (
                left.role.as_deref().unwrap_or_default(),
                left.declared_address.as_str(),
                left.declared_implementation_address
                    .as_deref()
                    .unwrap_or_default(),
            )
                .cmp(&(
                    right.role.as_deref().unwrap_or_default(),
                    right.declared_address.as_str(),
                    right
                        .declared_implementation_address
                        .as_deref()
                        .unwrap_or_default(),
                ))
        });
    }
    grouped
}

fn code_hash_observation_key(
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> (String, Uuid, String) {
    (chain.to_owned(), contract_instance_id, address.to_owned())
}

fn watched_contract_source_name(observation: &ManifestCodeHashObservation) -> &'static str {
    match observation.source {
        bigname_manifests::WatchedContractSource::ManifestRoot => "manifest_root",
        bigname_manifests::WatchedContractSource::ManifestContract => "manifest_contract",
        bigname_manifests::WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}

fn canonicality_state_from_view(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => anyhow::bail!("failed to parse manifest drift canonicality state {value}"),
    }
}

fn manifest_version_i64(manifest_version: u64) -> Result<i64> {
    i64::try_from(manifest_version).context("manifest_version does not fit in i64")
}

async fn load_normalized_event_counts_by_kind(pool: &PgPool) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized-event counts by kind")?;

    let mut counts = BTreeMap::new();
    for row in rows {
        let event_kind = row
            .try_get::<String, _>("event_kind")
            .context("missing event_kind from normalized-event count row")?;
        let event_count = row
            .try_get::<i64, _>("event_count")
            .context("missing event_count from normalized-event count row")?;
        counts.insert(
            event_kind,
            usize::try_from(event_count).context("normalized-event count does not fit in usize")?,
        );
    }

    Ok(counts)
}

#[cfg(test)]
mod tests;
