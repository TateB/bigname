use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, bail};
use bigname_manifests::{DiscoveryObservation, reconcile_discovery_observations};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Row, types::Uuid};

const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
#[cfg(test)]
const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
#[cfg(test)]
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
const RESOLVER_EDGE_KIND: &str = "resolver";
const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED: &str = "ens_v1_subregistry_changed";
const DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED: &str = "ens_v1_registry_resolver_changed";
const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
#[cfg(test)]
const ZERO_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1SubregistryDiscoverySyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub active_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
}

#[derive(Clone, Debug)]
struct RegistryRawLogRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
    contract_instance_id: Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    source_rank: i32,
}

#[derive(Clone, Debug)]
struct ObservedRegistryAssignment {
    observation_key: String,
    observation: DiscoveryObservation,
    raw_log: RegistryRawLogRow,
    discovery_kind: RegistryDiscoveryKind,
}

#[derive(Clone, Debug)]
struct ActiveRegistryEdge {
    observation_key: String,
    discovery_source: String,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryDiscoveryKind {
    Subregistry,
    Resolver,
}

impl RegistryDiscoveryKind {
    const fn edge_kind(self) -> &'static str {
        match self {
            Self::Subregistry => SUBREGISTRY_EDGE_KIND,
            Self::Resolver => RESOLVER_EDGE_KIND,
        }
    }

    const fn event_kind(self) -> &'static str {
        match self {
            Self::Subregistry => EVENT_KIND_SUBREGISTRY_CHANGED,
            Self::Resolver => EVENT_KIND_RESOLVER_CHANGED,
        }
    }

    const fn derivation_kind(self) -> &'static str {
        match self {
            Self::Subregistry => DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED,
            Self::Resolver => DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED,
        }
    }

    const fn source_event(self) -> &'static str {
        match self {
            Self::Subregistry => "NewOwner",
            Self::Resolver => "NewResolver",
        }
    }
}

pub async fn sync_ens_v1_subregistry_discovery(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let emitters = load_active_emitters(pool, chain).await?;
    let raw_logs = load_registry_raw_logs(pool, chain, &emitters).await?;
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, ObservedRegistryAssignment>::new();
    for raw_log in &raw_logs {
        let Some(assignment) = build_registry_assignment(raw_log, chain)? else {
            continue;
        };
        matched_log_count += 1;
        latest_assignments.insert(
            format!(
                "{}:{}",
                assignment.observation.discovery_source, assignment.observation_key
            ),
            assignment,
        );
    }

    let observations = latest_assignments
        .values()
        .map(|assignment| assignment.observation.clone())
        .collect::<Vec<_>>();
    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count: raw_logs.len(),
        matched_log_count,
        active_observation_count: observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
    };
    for discovery_source in &discovery_sources {
        let source_observations = observations
            .iter()
            .filter(|observation| observation.discovery_source == discovery_source.as_str())
            .cloned()
            .collect::<Vec<_>>();
        let source_reconciliation =
            reconcile_discovery_observations(pool, discovery_source, &source_observations).await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
    }

    let active_edges_by_observation_key =
        load_active_registry_edges_by_observation_key(pool, &discovery_sources).await?;
    let events = latest_assignments
        .values()
        .filter_map(|assignment| {
            build_registry_changed_event(
                assignment,
                active_edges_by_observation_key.get(&(
                    assignment.observation.discovery_source.clone(),
                    assignment.observation_key.clone(),
                )),
            )
            .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    upsert_normalized_events(pool, &events).await?;

    Ok(reconciliation)
}

fn build_registry_assignment(
    raw_log: &RegistryRawLogRow,
    chain: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    if topic0.eq_ignore_ascii_case(&new_owner_topic0()) {
        build_subregistry_assignment(raw_log, &ens_v1_subregistry_discovery_source(chain))
    } else if topic0.eq_ignore_ascii_case(&new_resolver_topic0()) {
        build_resolver_assignment(raw_log, &ens_v1_resolver_discovery_source(chain))
    } else {
        Ok(None)
    }
}

fn build_subregistry_assignment(
    raw_log: &RegistryRawLogRow,
    discovery_source: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let parent_node = raw_log
        .topics
        .get(1)
        .context("NewOwner log is missing indexed parent node topic")?;
    let labelhash = raw_log
        .topics
        .get(2)
        .context("NewOwner log is missing indexed labelhash topic")?;
    let child_node = child_node(parent_node, labelhash)?;
    let owner = decode_owner_address(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NewOwner owner payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(Some(ObservedRegistryAssignment {
        observation_key: child_node.clone(),
        observation: DiscoveryObservation {
            chain: raw_log.chain_id.clone(),
            from_address: raw_log.emitting_address.clone(),
            to_address: owner.clone(),
            edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(raw_log.block_number),
            active_from_block_hash: Some(raw_log.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "source": "raw_log",
                "source_event": "NewOwner",
                "observation_key": child_node,
                "parent_node": normalize_hex_32(parent_node)?,
                "labelhash": normalize_hex_32(labelhash)?,
                "owner": owner,
                "chain_id": raw_log.chain_id,
                "block_hash": raw_log.block_hash,
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "transaction_index": raw_log.transaction_index,
                "log_index": raw_log.log_index,
                "emitting_address": raw_log.emitting_address,
                "tombstone": owner == ZERO_ADDRESS,
            }),
        },
        raw_log: raw_log.clone(),
        discovery_kind: RegistryDiscoveryKind::Subregistry,
    }))
}

fn build_resolver_assignment(
    raw_log: &RegistryRawLogRow,
    discovery_source: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let node = raw_log
        .topics
        .get(1)
        .context("NewResolver log is missing indexed node topic")?;
    let node = normalize_hex_32(node)?;
    let resolver = decode_owner_address(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NewResolver resolver payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation_key = format!("resolver:{}:{node}", raw_log.emitting_address);

    Ok(Some(ObservedRegistryAssignment {
        observation_key: observation_key.clone(),
        observation: DiscoveryObservation {
            chain: raw_log.chain_id.clone(),
            from_address: raw_log.emitting_address.clone(),
            to_address: resolver.clone(),
            edge_kind: RESOLVER_EDGE_KIND.to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(raw_log.block_number),
            active_from_block_hash: Some(raw_log.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "source": "raw_log",
                "source_event": "NewResolver",
                "observation_key": observation_key,
                "node": node,
                "resolver": resolver,
                "resolver_profile_supported": false,
                "resolver_profile_status": "unsupported",
                "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
                "chain_id": raw_log.chain_id,
                "block_hash": raw_log.block_hash,
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "transaction_index": raw_log.transaction_index,
                "log_index": raw_log.log_index,
                "emitting_address": raw_log.emitting_address,
                "tombstone": resolver == ZERO_ADDRESS,
            }),
        },
        raw_log: raw_log.clone(),
        discovery_kind: RegistryDiscoveryKind::Resolver,
    }))
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
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_logs
        WHERE chain_id = $1
          AND lower(emitting_address) = ANY($2::TEXT[])
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number, transaction_index, log_index, lower(emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv1 registry raw logs for chain {chain}"))?;

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
                        "missing active emitter attribution for chain {chain} address {emitting_address}"
                    )
                })?;
            Ok(RegistryRawLogRow {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
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
            })
        })
        .collect()
}

async fn load_active_registry_edges_by_observation_key(
    pool: &PgPool,
    discovery_sources: &[String],
) -> Result<HashMap<(String, String), ActiveRegistryEdge>> {
    let rows = sqlx::query(
        r#"
        SELECT
            provenance ->> 'observation_key' AS observation_key,
            discovery_source,
            from_contract_instance_id,
            to_contract_instance_id
        FROM discovery_edges
        WHERE discovery_source = ANY($1::TEXT[])
          AND edge_kind IN ('subregistry', 'resolver')
          AND deactivated_at IS NULL
        "#,
    )
    .bind(discovery_sources)
    .fetch_all(pool)
    .await
    .context("failed to load active ENSv1 registry discovery edges")?;

    rows.into_iter()
        .map(|row| {
            let edge = ActiveRegistryEdge {
                observation_key: row
                    .try_get::<Option<String>, _>("observation_key")
                    .context("failed to read observation_key")?
                    .context("active ENSv1 registry edge is missing provenance.observation_key")?,
                discovery_source: row
                    .try_get("discovery_source")
                    .context("failed to read discovery_source")?,
                from_contract_instance_id: row
                    .try_get("from_contract_instance_id")
                    .context("failed to read from_contract_instance_id")?,
                to_contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("failed to read to_contract_instance_id")?,
            };
            Ok((
                (edge.discovery_source.clone(), edge.observation_key.clone()),
                edge,
            ))
        })
        .collect()
}

fn build_registry_changed_event(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<Option<NormalizedEvent>> {
    if assignment.observation.to_address != ZERO_ADDRESS && active_edge.is_none() {
        return Ok(None);
    }

    let after_state = match assignment.discovery_kind {
        RegistryDiscoveryKind::Subregistry => {
            build_subregistry_after_state(assignment, active_edge)?
        }
        RegistryDiscoveryKind::Resolver => build_resolver_after_state(assignment, active_edge)?,
    };
    Ok(Some(NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}:{}:{}:{}",
            assignment.discovery_kind.derivation_kind(),
            assignment.raw_log.source_manifest_id,
            assignment.raw_log.block_hash,
            assignment.raw_log.transaction_hash,
            assignment.raw_log.log_index,
            assignment.raw_log.emitting_address
        ),
        namespace: assignment.raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: assignment.discovery_kind.event_kind().to_owned(),
        source_family: assignment.raw_log.source_family.clone(),
        manifest_version: assignment.raw_log.manifest_version,
        source_manifest_id: Some(assignment.raw_log.source_manifest_id),
        chain_id: Some(assignment.raw_log.chain_id.clone()),
        block_number: Some(assignment.raw_log.block_number),
        block_hash: Some(assignment.raw_log.block_hash.clone()),
        transaction_hash: Some(assignment.raw_log.transaction_hash.clone()),
        log_index: Some(assignment.raw_log.log_index),
        raw_fact_ref: serde_json::json!({
            "kind": "raw_log",
            "chain_id": assignment.raw_log.chain_id,
            "block_hash": assignment.raw_log.block_hash,
            "block_number": assignment.raw_log.block_number,
            "transaction_hash": assignment.raw_log.transaction_hash,
            "transaction_index": assignment.raw_log.transaction_index,
            "log_index": assignment.raw_log.log_index,
            "emitting_address": assignment.raw_log.emitting_address,
            "topic0": assignment.raw_log.topics.first().cloned(),
            "topic1": assignment.raw_log.topics.get(1).cloned(),
            "topic2": assignment.raw_log.topics.get(2).cloned(),
            "data_hex": hex_string(&assignment.raw_log.data),
        }),
        derivation_kind: assignment.discovery_kind.derivation_kind().to_owned(),
        canonicality_state: assignment.raw_log.canonicality_state,
        before_state: serde_json::json!({}),
        after_state,
    }))
}

fn build_subregistry_after_state(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<serde_json::Value> {
    let parent_node = assignment
        .observation
        .provenance
        .get("parent_node")
        .and_then(|value| value.as_str())
        .context("ENSv1 subregistry observation is missing provenance.parent_node")?;
    let labelhash = assignment
        .observation
        .provenance
        .get("labelhash")
        .and_then(|value| value.as_str())
        .context("ENSv1 subregistry observation is missing provenance.labelhash")?;
    let child_node = assignment
        .observation
        .provenance
        .get("observation_key")
        .and_then(|value| value.as_str())
        .context("ENSv1 subregistry observation is missing provenance.observation_key")?;
    let owner = assignment
        .observation
        .provenance
        .get("owner")
        .and_then(|value| value.as_str())
        .context("ENSv1 subregistry observation is missing provenance.owner")?;
    let tombstone = assignment.observation.to_address == ZERO_ADDRESS;

    Ok(serde_json::json!({
        "source_event": assignment.discovery_kind.source_event(),
        "discovery_source": assignment.observation.discovery_source,
        "edge_kind": assignment.discovery_kind.edge_kind(),
        "observation_key": assignment.observation_key,
        "parent_node": parent_node,
        "labelhash": labelhash,
        "child_node": child_node,
        "emitting_address": assignment.raw_log.emitting_address,
        "owner": owner,
        "tombstone": tombstone,
        "from_contract_instance_id": active_edge
            .map(|edge| edge.from_contract_instance_id.to_string())
            .unwrap_or_else(|| assignment.raw_log.emitting_contract_instance_id.to_string()),
        "to_contract_instance_id": active_edge.map(|edge| edge.to_contract_instance_id.to_string()),
        "active_edge": !tombstone && active_edge.is_some(),
    }))
}

fn build_resolver_after_state(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<serde_json::Value> {
    let node = assignment
        .observation
        .provenance
        .get("node")
        .and_then(|value| value.as_str())
        .context("ENSv1 resolver observation is missing provenance.node")?;
    let resolver = assignment
        .observation
        .provenance
        .get("resolver")
        .and_then(|value| value.as_str())
        .context("ENSv1 resolver observation is missing provenance.resolver")?;
    let tombstone = assignment.observation.to_address == ZERO_ADDRESS;

    Ok(serde_json::json!({
        "source_event": assignment.discovery_kind.source_event(),
        "discovery_source": assignment.observation.discovery_source,
        "edge_kind": assignment.discovery_kind.edge_kind(),
        "observation_key": assignment.observation_key,
        "node": node,
        "emitting_address": assignment.raw_log.emitting_address,
        "resolver": null_if_zero_address(resolver),
        "raw_resolver": resolver,
        "tombstone": tombstone,
        "from_contract_instance_id": active_edge
            .map(|edge| edge.from_contract_instance_id.to_string())
            .unwrap_or_else(|| assignment.raw_log.emitting_contract_instance_id.to_string()),
        "to_contract_instance_id": active_edge.map(|edge| edge.to_contract_instance_id.to_string()),
        "active_edge": !tombstone && active_edge.is_some(),
        "resolver_profile_supported": false,
        "resolver_profile_status": "unsupported",
        "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
    }))
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            contract_instance_id,
            address,
            source_rank
        FROM (
            SELECT
                mv.chain AS chain,
                mv.namespace AS namespace,
                mv.source_family AS source_family,
                mv.manifest_version AS manifest_version,
                mv.manifest_id AS source_manifest_id,
                mci.contract_instance_id AS contract_instance_id,
                cia.address AS address,
                CASE WHEN mci.declaration_kind = 'root' THEN 0 ELSE 1 END::INT AS source_rank
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND mv.source_family IN ($2, $3)

            UNION

            SELECT
                de.chain_id AS chain,
                mv.namespace AS namespace,
                mv.source_family AS source_family,
                mv.manifest_version AS manifest_version,
                de.source_manifest_id AS source_manifest_id,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address,
                2::INT AS source_rank
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.chain_id = $1
              AND de.edge_kind = $4
              AND mv.source_family IN ($2, $3)
        ) registry_emitters
        ORDER BY lower(address), source_rank, source_manifest_id, contract_instance_id
        "#,
    )
    .bind(chain)
    .bind(ENS_V1_REGISTRY_SOURCE_FAMILY)
    .bind(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    .bind(SUBREGISTRY_EDGE_KIND)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load active ENSv1 registry emitters for {chain}"))?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for row in rows {
        let address = normalize_address(&row.try_get::<String, _>("address")?);
        let candidate = ActiveEmitter {
            address,
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("missing registry emitter contract_instance_id")?,
            source_manifest_id: row
                .try_get("source_manifest_id")
                .context("missing registry emitter source_manifest_id")?,
            namespace: row
                .try_get("namespace")
                .context("missing registry emitter namespace")?,
            source_family: row
                .try_get("source_family")
                .context("missing registry emitter source_family")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("missing registry emitter manifest_version")?,
            source_rank: row
                .try_get("source_rank")
                .context("missing registry emitter source_rank")?,
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

fn decode_owner_address(data: &[u8]) -> Result<String> {
    if data.len() < 32 {
        bail!("NewOwner log data must be at least 32 bytes");
    }

    Ok(format!("0x{}", hex_string(&data[12..32])))
}

fn child_node(parent_node: &str, labelhash: &str) -> Result<String> {
    let parent_node = decode_hex_32(parent_node)?;
    let labelhash = decode_hex_32(labelhash)?;
    let mut hasher = Keccak256::new();
    hasher.update(parent_node);
    hasher.update(labelhash);
    Ok(format!("0x{}", hex_string(hasher.finalize())))
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized.as_bytes()[2..].chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex topic chunk must be utf-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {value}");
    }
    Ok(normalized)
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
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

fn ens_v1_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v1_registry_new_owner:{chain}")
}

fn ens_v1_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v1_registry_resolver:{chain}")
}

fn new_owner_topic0() -> String {
    keccak_signature_hex(NEW_OWNER_SIGNATURE)
}

fn new_resolver_topic0() -> String {
    keccak_signature_hex(NEW_RESOLVER_SIGNATURE)
}

fn keccak_signature_hex(signature: &str) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    format!("0x{}", hex_string(hasher.finalize()))
}

fn null_if_zero_address(address: &str) -> Option<String> {
    if normalize_address(address) == ZERO_ADDRESS {
        None
    } else {
        Some(normalize_address(address))
    }
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
