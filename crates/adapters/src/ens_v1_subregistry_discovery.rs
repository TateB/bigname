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
    Ok(format!("0x{}", hex_string(&hasher.finalize())))
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized[2..].as_bytes().chunks(2).enumerate() {
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
    format!("0x{}", hex_string(&hasher.finalize()))
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
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use bigname_manifests::{
        WatchedChainPlan, WatchedContractSource, WatchedSourceSelector, load_repository,
        load_watched_chain_plan, load_watched_contract_summary, load_watched_contracts,
        load_watched_source_selector_plan, sync_repository,
    };
    use bigname_storage::{
        RawBlock, RawLog, default_database_url, load_normalized_events_by_namespace,
        upsert_raw_blocks, upsert_raw_logs,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        query_scalar,
        types::time::OffsetDateTime,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);
    const TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Result<Self> {
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "bigname-ensv1-subregistry-{}-{}-{sequence}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("system clock is before unix epoch")?
                    .as_nanos()
            ));
            fs::create_dir_all(&root)
                .with_context(|| format!("failed to create test directory {}", root.display()))?;
            Ok(Self { path: root })
        }

        fn write_manifest(
            &self,
            namespace: &str,
            source_family: &str,
            version: &str,
            contents: &str,
        ) -> Result<PathBuf> {
            let directory = self.path.join(namespace).join(source_family);
            fs::create_dir_all(&directory).with_context(|| {
                format!(
                    "failed to create manifest directory {}",
                    directory.display()
                )
            })?;
            let path = directory.join(format!("{version}.toml"));
            fs::write(&path, contents)
                .with_context(|| format!("failed to write manifest {}", path.display()))?;
            Ok(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

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
            let connect_options: PgConnectOptions = database_url.parse().with_context(|| {
                "failed to parse database URL for ENSv1 subregistry adapter tests".to_owned()
            })?;
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(connect_options.clone().database("postgres"))
                .await
                .context("failed to connect admin test database pool")?;

            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_ensv1_subregistry_{}_{}_{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("system clock is before unix epoch")?
                    .as_nanos(),
                sequence
            );
            sqlx::query(&format!(r#"CREATE DATABASE "{database_name}""#))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let mut database_options = connect_options.database(&database_name);
            database_options = database_options.application_name("bigname-ensv1-subregistry-tests");
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(database_options)
                .await
                .with_context(|| format!("failed to connect test database {database_name}"))?;

            TEST_MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for ENSv1 subregistry adapter tests")?;

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

    fn manifest_contents(include_discovery_rule: bool) -> String {
        manifest_contents_for_registry(
            "ens",
            ENS_V1_REGISTRY_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
            "ENSRegistry",
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            include_discovery_rule,
        )
    }

    fn manifest_contents_for_registry(
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        root_name: &str,
        root_address: &str,
        include_discovery_rule: bool,
    ) -> String {
        let discovery_rule = if include_discovery_rule {
            r#"
[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"
"#
        } else {
            r#"
[[discovery_rules]]
edge_kind = "subregistry"
from_role = "wrapper"
admission = "reachable_from_root"
"#
        };
        format!(
            r#"
manifest_version = 1
namespace = "{namespace}"
source_family = "{source_family}"
chain = "{chain}"
deployment_epoch = "{deployment_epoch}"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "{root_name}"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{root_address}"
proxy_kind = "none"
{discovery_rule}
"#
        )
    }

    fn resolver_manifest_contents_for_family(
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
    ) -> String {
        format!(
            r#"
manifest_version = 1
namespace = "{namespace}"
source_family = "{source_family}"
chain = "{chain}"
deployment_epoch = "{deployment_epoch}"
rollout_status = "active"
normalizer_version = "uts46-v1"
roots = []
contracts = []
discovery_rules = []

[capability_flags]
"#
        )
    }

    async fn insert_raw_new_owner_log(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        emitting_address: &str,
        owner: &str,
        canonicality_state: CanonicalityState,
    ) -> Result<()> {
        insert_raw_new_owner_log_with_key(
            pool,
            chain_id,
            block_hash,
            block_number,
            emitting_address,
            owner,
            ZERO_NODE,
            "eth",
            canonicality_state,
        )
        .await
    }

    async fn insert_raw_new_owner_log_with_key(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        emitting_address: &str,
        owner: &str,
        parent_node: &str,
        label: &str,
        canonicality_state: CanonicalityState,
    ) -> Result<()> {
        upsert_raw_blocks(
            pool,
            &[RawBlock {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                parent_hash: None,
                block_number,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state,
            }],
        )
        .await?;

        upsert_raw_logs(
            pool,
            &[RawLog {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: format!("0xtx{block_number:02x}"),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitting_address.to_owned(),
                topics: vec![
                    new_owner_topic0(),
                    parent_node.to_owned(),
                    labelhash_hex(label),
                ],
                data: encode_new_owner_log_data(owner),
                canonicality_state,
            }],
        )
        .await?;

        Ok(())
    }

    async fn insert_raw_new_resolver_log(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        emitting_address: &str,
        resolver: &str,
        node: &str,
        canonicality_state: CanonicalityState,
    ) -> Result<()> {
        upsert_raw_blocks(
            pool,
            &[RawBlock {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                parent_hash: None,
                block_number,
                block_timestamp: OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state,
            }],
        )
        .await?;

        upsert_raw_logs(
            pool,
            &[RawLog {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: format!("0xtx{block_number:02x}"),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitting_address.to_owned(),
                topics: vec![new_resolver_topic0(), normalize_hex_32(node)?],
                data: encode_registry_new_resolver_log_data(resolver),
                canonicality_state,
            }],
        )
        .await?;

        Ok(())
    }

    async fn load_contract_instance_for_address(
        pool: &PgPool,
        chain: &str,
        address: &str,
    ) -> Result<Uuid> {
        query_scalar::<_, Uuid>(
            r#"
            SELECT contract_instance_id
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
            "#,
        )
        .bind(chain)
        .bind(normalize_address(address))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to load contract instance for {chain} {address}"))
    }

    async fn active_manifest_id_for_source_family(
        pool: &PgPool,
        namespace: &str,
        source_family: &str,
    ) -> Result<i64> {
        query_scalar::<_, i64>(
            r#"
            SELECT manifest_id
            FROM manifest_versions
            WHERE namespace = $1
              AND source_family = $2
              AND rollout_status = 'active'
            "#,
        )
        .bind(namespace)
        .bind(source_family)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to load active manifest_id for {source_family}"))
    }

    fn labelhash_hex(label: &str) -> String {
        let mut hasher = Keccak256::new();
        hasher.update(label.as_bytes());
        format!("0x{}", hex_string(hasher.finalize()))
    }

    fn base_eth_node() -> Result<String> {
        child_node(
            &child_node(ZERO_NODE, &labelhash_hex("eth"))?,
            &labelhash_hex("base"),
        )
    }

    fn encode_new_owner_log_data(owner: &str) -> Vec<u8> {
        abi_word_address(owner).to_vec()
    }

    fn encode_registry_new_resolver_log_data(resolver: &str) -> Vec<u8> {
        abi_word_address(resolver).to_vec()
    }

    fn abi_word_address(value: &str) -> [u8; 32] {
        let value = value.strip_prefix("0x").unwrap_or(value);
        assert_eq!(value.len(), 40, "test address must be 20 bytes");
        let mut word = [0u8; 32];
        for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
            let hex = std::str::from_utf8(chunk).expect("test address chunk must be utf-8");
            word[12 + index] =
                u8::from_str_radix(hex, 16).expect("test address chunk must be valid hex");
        }
        word
    }

    #[tokio::test]
    async fn canonical_new_owner_log_persists_one_active_subregistry_edge_and_expands_watch_plan()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(
            summary,
            EnsV1SubregistryDiscoverySyncSummary {
                scanned_log_count: 1,
                matched_log_count: 1,
                active_observation_count: 1,
                active_edge_count: 1,
                admitted_edge_count: 1,
                inserted_edge_count: 1,
                deactivated_edge_count: 0,
            }
        );

        let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            1
        );
        let discovered_contract_instance_id = load_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000cc",
        )
        .await?;
        assert_eq!(
            query_scalar::<_, Uuid>(
                "SELECT to_contract_instance_id FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            discovered_contract_instance_id
        );
        let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(normalized_events.len(), 1);
        assert_eq!(
            normalized_events[0].event_kind,
            EVENT_KIND_SUBREGISTRY_CHANGED
        );
        assert_eq!(
            normalized_events[0].block_hash.as_deref(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            normalized_events[0].after_state["owner"].as_str(),
            Some("0x00000000000000000000000000000000000000cc")
        );
        assert_eq!(
            normalized_events[0].after_state["tombstone"].as_bool(),
            Some(false)
        );
        assert_eq!(
            normalized_events[0].after_state["to_contract_instance_id"].as_str(),
            Some(discovered_contract_instance_id.to_string().as_str())
        );
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .len(),
            1
        );

        let watched_summary = load_watched_contract_summary(database.pool()).await?;
        assert_eq!(watched_summary.unique_contract_count, 2);
        assert_eq!(watched_summary.manifest_root_count, 1);
        assert_eq!(watched_summary.manifest_contract_count, 1);
        assert_eq!(watched_summary.discovery_edge_count, 1);

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );
        database.cleanup().await
    }

    #[tokio::test]
    async fn basenames_finalized_new_owner_log_emits_basenames_subregistry_event_idempotently()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "basenames",
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            "v1",
            &manifest_contents_for_registry(
                "basenames",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "base-mainnet",
                "basenames_v1",
                "BasenamesRegistry",
                "0x00000000000000000000000000000000000000bb",
                true,
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log_with_key(
            database.pool(),
            "base-mainnet",
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            42,
            "0x00000000000000000000000000000000000000bb",
            "0x00000000000000000000000000000000000000cc",
            &base_eth_node()?,
            "alice",
            CanonicalityState::Finalized,
        )
        .await?;

        let first = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
        assert_eq!(
            first,
            EnsV1SubregistryDiscoverySyncSummary {
                scanned_log_count: 1,
                matched_log_count: 1,
                active_observation_count: 1,
                active_edge_count: 1,
                admitted_edge_count: 1,
                inserted_edge_count: 1,
                deactivated_edge_count: 0,
            }
        );

        let second = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
        assert_eq!(
            second,
            EnsV1SubregistryDiscoverySyncSummary {
                scanned_log_count: 1,
                matched_log_count: 1,
                active_observation_count: 1,
                active_edge_count: 1,
                admitted_edge_count: 1,
                inserted_edge_count: 0,
                deactivated_edge_count: 0,
            }
        );

        let discovery_source = ens_v1_subregistry_discovery_source("base-mainnet");
        let parent_node = base_eth_node()?;
        let discovered_contract_instance_id = load_contract_instance_for_address(
            database.pool(),
            "base-mainnet",
            "0x00000000000000000000000000000000000000cc",
        )
        .await?;
        let normalized_events =
            load_normalized_events_by_namespace(database.pool(), "basenames").await?;
        assert_eq!(normalized_events.len(), 1);
        assert_eq!(normalized_events[0].namespace, "basenames");
        assert_eq!(
            normalized_events[0].canonicality_state,
            CanonicalityState::Finalized
        );
        assert_eq!(
            normalized_events[0].after_state["parent_node"].as_str(),
            Some(parent_node.as_str())
        );
        assert_eq!(
            normalized_events[0].after_state["active_edge"].as_bool(),
            Some(true)
        );
        assert_eq!(
            normalized_events[0].after_state["to_contract_instance_id"].as_str(),
            Some(discovered_contract_instance_id.to_string().as_str())
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            1
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000bb".to_owned(),
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );
        database.cleanup().await
    }

    #[tokio::test]
    async fn canonical_new_resolver_log_persists_resolver_edge_without_profile_support()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        test_dir.write_manifest(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "v1",
            &resolver_manifest_contents_for_family(
                "ens",
                ENS_V1_RESOLVER_SOURCE_FAMILY,
                "ethereum-mainnet",
                "ens_v1",
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        let registry_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            "ens",
            ENS_V1_REGISTRY_SOURCE_FAMILY,
        )
        .await?;
        let resolver_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
        )
        .await?;
        let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
        insert_raw_new_resolver_log(
            database.pool(),
            "ethereum-mainnet",
            "0x9999999999999999999999999999999999999999999999999999999999999999",
            58,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            &node,
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(
            summary,
            EnsV1SubregistryDiscoverySyncSummary {
                scanned_log_count: 1,
                matched_log_count: 1,
                active_observation_count: 1,
                active_edge_count: 1,
                admitted_edge_count: 1,
                inserted_edge_count: 1,
                deactivated_edge_count: 0,
            }
        );

        let discovery_source = ens_v1_resolver_discovery_source("ethereum-mainnet");
        let discovered_contract_instance_id = load_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000cc",
        )
        .await?;
        let discovery_edge = sqlx::query(
            r#"
            SELECT to_contract_instance_id, source_manifest_id, provenance
            FROM discovery_edges
            WHERE discovery_source = $1
              AND edge_kind = 'resolver'
              AND deactivated_at IS NULL
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            discovery_edge.try_get::<Uuid, _>("to_contract_instance_id")?,
            discovered_contract_instance_id
        );
        assert_eq!(
            discovery_edge
                .try_get::<Option<i64>, _>("source_manifest_id")?
                .expect("resolver edge must retain registry source manifest provenance"),
            registry_manifest_id
        );
        assert!(
            !discovery_edge
                .try_get::<serde_json::Value, _>("provenance")?
                .as_object()
                .expect("resolver discovery provenance must be an object")
                .contains_key("propagated_role")
        );

        let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(normalized_events.len(), 1);
        assert_eq!(normalized_events[0].event_kind, EVENT_KIND_RESOLVER_CHANGED);
        assert_eq!(
            normalized_events[0].source_family,
            ENS_V1_REGISTRY_SOURCE_FAMILY
        );
        assert_eq!(
            normalized_events[0].derivation_kind,
            DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED
        );
        assert_eq!(
            normalized_events[0].after_state["resolver"].as_str(),
            Some("0x00000000000000000000000000000000000000cc")
        );
        assert_eq!(
            normalized_events[0].after_state["resolver_profile_supported"].as_bool(),
            Some(false)
        );
        assert_eq!(
            normalized_events[0].after_state["resolver_profile_status"].as_str(),
            Some("unsupported")
        );
        assert_eq!(
            normalized_events[0].after_state["active_edge"].as_bool(),
            Some(true)
        );
        assert_eq!(
            normalized_events[0].after_state["to_contract_instance_id"].as_str(),
            Some(discovered_contract_instance_id.to_string().as_str())
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );
        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-mainnet"
                && contract.address == "0x00000000000000000000000000000000000000cc"
                && contract.source == WatchedContractSource::DiscoveryEdge
                && contract.source_family == ENS_V1_RESOLVER_SOURCE_FAMILY
                && contract.source_manifest_id == Some(resolver_manifest_id)
        }));
        let resolver_source_plan = load_watched_source_selector_plan(
            database.pool(),
            "ethereum-mainnet",
            WatchedSourceSelector::SourceFamily(ENS_V1_RESOLVER_SOURCE_FAMILY.to_owned()),
            58,
            58,
        )
        .await?;
        assert_eq!(resolver_source_plan.selected_targets.len(), 1);
        assert_eq!(
            resolver_source_plan.selected_targets[0].source_family,
            ENS_V1_RESOLVER_SOURCE_FAMILY
        );
        assert_eq!(
            resolver_source_plan.selected_targets[0].contract_instance_id,
            discovered_contract_instance_id
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn new_resolver_target_is_not_registry_discovery_emitter() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        test_dir.write_manifest(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "v1",
            &resolver_manifest_contents_for_family(
                "ens",
                ENS_V1_RESOLVER_SOURCE_FAMILY,
                "ethereum-mainnet",
                "ens_v1",
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
        let resolver_address = "0x00000000000000000000000000000000000000CC";
        insert_raw_new_resolver_log(
            database.pool(),
            "ethereum-mainnet",
            "0x8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a",
            70,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver_address,
            &node,
            CanonicalityState::Canonical,
        )
        .await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b",
            71,
            resolver_address,
            "0x00000000000000000000000000000000000000DD",
            CanonicalityState::Canonical,
        )
        .await?;

        let first = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first.scanned_log_count, 1);
        assert_eq!(first.active_edge_count, 1);
        assert_eq!(first.inserted_edge_count, 1);

        let second = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second.scanned_log_count, 1);
        assert_eq!(second.active_edge_count, 1);
        assert_eq!(second.inserted_edge_count, 0);
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn basenames_new_resolver_log_admits_resolver_watch_target_without_profile_support()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "basenames",
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            "v1",
            &manifest_contents_for_registry(
                "basenames",
                BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
                "base-mainnet",
                "basenames_v1",
                "BasenamesRegistry",
                "0x00000000000000000000000000000000000000bb",
                true,
            ),
        )?;
        test_dir.write_manifest(
            "basenames",
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            "v1",
            &resolver_manifest_contents_for_family(
                "basenames",
                BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
                "base-mainnet",
                "basenames_v1",
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        let registry_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            "basenames",
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
        )
        .await?;
        let resolver_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            "basenames",
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        )
        .await?;
        let node = child_node(&base_eth_node()?, &labelhash_hex("alice"))?;
        insert_raw_new_resolver_log(
            database.pool(),
            "base-mainnet",
            "0x9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a",
            59,
            "0x00000000000000000000000000000000000000bb",
            "0x00000000000000000000000000000000000000cc",
            &node,
            CanonicalityState::Finalized,
        )
        .await?;

        let summary = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
        assert_eq!(summary.scanned_log_count, 1);
        assert_eq!(summary.matched_log_count, 1);
        assert_eq!(summary.active_observation_count, 1);
        assert_eq!(summary.active_edge_count, 1);
        assert_eq!(summary.admitted_edge_count, 1);
        assert_eq!(summary.inserted_edge_count, 1);

        let normalized_events =
            load_normalized_events_by_namespace(database.pool(), "basenames").await?;
        assert_eq!(normalized_events.len(), 1);
        assert_eq!(normalized_events[0].namespace, "basenames");
        assert_eq!(normalized_events[0].event_kind, EVENT_KIND_RESOLVER_CHANGED);
        assert_eq!(
            normalized_events[0].canonicality_state,
            CanonicalityState::Finalized
        );
        assert_eq!(
            normalized_events[0].source_family,
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
        );
        assert_eq!(
            normalized_events[0].after_state["node"].as_str(),
            Some(node.as_str())
        );
        assert_eq!(
            normalized_events[0].after_state["resolver_profile_supported"].as_bool(),
            Some(false)
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000bb".to_owned(),
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );
        let discovered_contract_instance_id = load_contract_instance_for_address(
            database.pool(),
            "base-mainnet",
            "0x00000000000000000000000000000000000000cc",
        )
        .await?;
        let discovery_edge = sqlx::query(
            r#"
            SELECT to_contract_instance_id, source_manifest_id, provenance
            FROM discovery_edges
            WHERE discovery_source = $1
              AND edge_kind = 'resolver'
              AND deactivated_at IS NULL
            "#,
        )
        .bind(ens_v1_resolver_discovery_source("base-mainnet"))
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            discovery_edge.try_get::<Uuid, _>("to_contract_instance_id")?,
            discovered_contract_instance_id
        );
        assert_eq!(
            discovery_edge
                .try_get::<Option<i64>, _>("source_manifest_id")?
                .expect("resolver edge must retain registry source manifest provenance"),
            registry_manifest_id
        );
        assert!(
            !discovery_edge
                .try_get::<serde_json::Value, _>("provenance")?
                .as_object()
                .expect("resolver discovery provenance must be an object")
                .contains_key("propagated_role")
        );
        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "base-mainnet"
                && contract.address == "0x00000000000000000000000000000000000000cc"
                && contract.source == WatchedContractSource::DiscoveryEdge
                && contract.source_family == BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
                && contract.source_manifest_id == Some(resolver_manifest_id)
        }));
        let resolver_source_plan = load_watched_source_selector_plan(
            database.pool(),
            "base-mainnet",
            WatchedSourceSelector::SourceFamily(BASENAMES_BASE_RESOLVER_SOURCE_FAMILY.to_owned()),
            59,
            59,
        )
        .await?;
        assert_eq!(resolver_source_plan.selected_targets.len(), 1);
        assert_eq!(
            resolver_source_plan.selected_targets[0].source_family,
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
        );
        assert_eq!(
            resolver_source_plan.selected_targets[0].contract_instance_id,
            discovered_contract_instance_id
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn zero_resolver_update_closes_resolver_edge_without_closing_subregistry_edge()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b",
            60,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000DD",
            CanonicalityState::Canonical,
        )
        .await?;
        insert_raw_new_resolver_log(
            database.pool(),
            "ethereum-mainnet",
            "0x9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c",
            61,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            &node,
            CanonicalityState::Canonical,
        )
        .await?;
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

        insert_raw_new_resolver_log(
            database.pool(),
            "ethereum-mainnet",
            "0x9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d",
            62,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            ZERO_ADDRESS,
            &node,
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.active_observation_count, 1);
        assert_eq!(summary.active_edge_count, 1);
        assert_eq!(summary.inserted_edge_count, 0);
        assert_eq!(summary.deactivated_edge_count, 1);
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );

        let resolver_tombstone = sqlx::query(
            r#"
            SELECT active_to_block_number, active_to_block_hash
            FROM discovery_edges
            WHERE edge_kind = 'resolver'
              AND deactivated_at IS NOT NULL
            ORDER BY discovery_edge_id DESC
            LIMIT 1
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            resolver_tombstone.try_get::<Option<i64>, _>("active_to_block_number")?,
            Some(62)
        );
        assert_eq!(
            resolver_tombstone.try_get::<Option<String>, _>("active_to_block_hash")?,
            Some("0x9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d".to_owned())
        );
        let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(normalized_events.len(), 3);
        assert_eq!(normalized_events[2].event_kind, EVENT_KIND_RESOLVER_CHANGED);
        assert!(normalized_events[2].after_state["resolver"].is_null());
        assert_eq!(
            normalized_events[2].after_state["raw_resolver"].as_str(),
            Some(ZERO_ADDRESS)
        );
        assert_eq!(
            normalized_events[2].after_state["active_edge"].as_bool(),
            Some(false)
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000dd".to_owned(),
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_extends_transitively_from_discovered_subregistries()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            50,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

        let first_child_node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
        insert_raw_new_owner_log_with_key(
            database.pool(),
            "ethereum-mainnet",
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            51,
            "0x00000000000000000000000000000000000000CC",
            "0x00000000000000000000000000000000000000DD",
            &first_child_node,
            "sub",
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.active_observation_count, 2);
        assert_eq!(summary.active_edge_count, 2);
        assert_eq!(summary.admitted_edge_count, 2);
        assert_eq!(summary.inserted_edge_count, 1);
        assert_eq!(summary.deactivated_edge_count, 0);

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                    "0x00000000000000000000000000000000000000dd".to_owned(),
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 2,
            }]
        );
        let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(normalized_events.len(), 2);
        assert_eq!(
            normalized_events[1].after_state["emitting_address"].as_str(),
            Some("0x00000000000000000000000000000000000000cc")
        );
        assert_eq!(
            normalized_events[1].after_state["owner"].as_str(),
            Some("0x00000000000000000000000000000000000000dd")
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_accepts_finalized_logs() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x3333333333333333333333333333333333333333333333333333333333333333",
            52,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000EE",
            CanonicalityState::Finalized,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.scanned_log_count, 1);
        assert_eq!(summary.matched_log_count, 1);
        assert_eq!(summary.active_edge_count, 1);
        assert_eq!(summary.admitted_edge_count, 1);

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_skips_observed_and_orphaned_logs() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            43,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Observed,
        )
        .await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            44,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000DD",
            CanonicalityState::Orphaned,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.scanned_log_count, 0);
        assert_eq!(summary.matched_log_count, 0);
        assert_eq!(summary.active_observation_count, 0);
        assert_eq!(summary.active_edge_count, 0);
        assert_eq!(summary.admitted_edge_count, 0);

        let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            0
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_clears_zero_owner_edges_deterministically()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x4444444444444444444444444444444444444444444444444444444444444444",
            53,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x5555555555555555555555555555555555555555555555555555555555555555",
            54,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            ZERO_ADDRESS,
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.active_observation_count, 0);
        assert_eq!(summary.active_edge_count, 0);
        assert_eq!(summary.inserted_edge_count, 0);
        assert_eq!(summary.deactivated_edge_count, 1);
        let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(normalized_events.len(), 2);
        assert_eq!(
            normalized_events[1].event_kind,
            EVENT_KIND_SUBREGISTRY_CHANGED
        );
        assert_eq!(
            normalized_events[1].after_state["owner"].as_str(),
            Some(ZERO_ADDRESS)
        );
        assert_eq!(
            normalized_events[1].after_state["tombstone"].as_bool(),
            Some(true)
        );

        let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
        let cleared_edge = sqlx::query(
            r#"
            SELECT active_to_block_number, active_to_block_hash
            FROM discovery_edges
            WHERE discovery_source = $1
            ORDER BY discovery_edge_id DESC
            LIMIT 1
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            cleared_edge.try_get::<Option<i64>, _>("active_to_block_number")?,
            Some(54)
        );
        assert_eq!(
            cleared_edge.try_get::<Option<String>, _>("active_to_block_hash")?,
            Some("0x5555555555555555555555555555555555555555555555555555555555555555".to_owned())
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
            }]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_cascades_descendant_teardown_in_same_sync()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x6666666666666666666666666666666666666666666666666666666666666666",
            55,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

        let first_child_node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
        insert_raw_new_owner_log_with_key(
            database.pool(),
            "ethereum-mainnet",
            "0x7777777777777777777777777777777777777777777777777777777777777777",
            56,
            "0x00000000000000000000000000000000000000CC",
            "0x00000000000000000000000000000000000000DD",
            &first_child_node,
            "sub",
            CanonicalityState::Canonical,
        )
        .await?;
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0x8888888888888888888888888888888888888888888888888888888888888888",
            57,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            ZERO_ADDRESS,
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.active_observation_count, 1);
        assert_eq!(summary.active_edge_count, 0);
        assert_eq!(summary.inserted_edge_count, 0);
        assert_eq!(summary.deactivated_edge_count, 2);
        assert_eq!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .len(),
            3
        );

        let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
        let ended_edges = sqlx::query(
            r#"
            SELECT active_to_block_number, active_to_block_hash
            FROM discovery_edges
            WHERE discovery_source = $1
              AND deactivated_at IS NOT NULL
            ORDER BY discovery_edge_id
            "#,
        )
        .bind(&discovery_source)
        .fetch_all(database.pool())
        .await?;
        assert_eq!(ended_edges.len(), 2);
        for edge in ended_edges {
            assert_eq!(
                edge.try_get::<Option<i64>, _>("active_to_block_number")?,
                Some(57)
            );
            assert_eq!(
                edge.try_get::<Option<String>, _>("active_to_block_hash")?,
                Some(
                    "0x8888888888888888888888888888888888888888888888888888888888888888".to_owned()
                )
            );
        }

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
            }]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_reconciles_reassigned_children_to_one_active_edge()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            46,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;

        let first_summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(first_summary.active_edge_count, 1);
        assert_eq!(first_summary.inserted_edge_count, 1);
        assert_eq!(first_summary.deactivated_edge_count, 0);

        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            47,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000DD",
            CanonicalityState::Canonical,
        )
        .await?;

        let second_summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(second_summary.scanned_log_count, 2);
        assert_eq!(second_summary.matched_log_count, 2);
        assert_eq!(second_summary.active_observation_count, 1);
        assert_eq!(second_summary.active_edge_count, 1);
        assert_eq!(second_summary.admitted_edge_count, 1);
        assert_eq!(second_summary.inserted_edge_count, 1);
        assert_eq!(second_summary.deactivated_edge_count, 1);

        let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1"
            )
            .bind(&discovery_source)
            .fetch_one(database.pool())
            .await?,
            2
        );
        let deactivated_edge = sqlx::query(
            r#"
            SELECT active_to_block_number, active_to_block_hash
            FROM discovery_edges
            WHERE discovery_source = $1
              AND deactivated_at IS NOT NULL
            ORDER BY discovery_edge_id DESC
            LIMIT 1
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            deactivated_edge.try_get::<Option<i64>, _>("active_to_block_number")?,
            Some(47)
        );
        assert_eq!(
            deactivated_edge.try_get::<Option<String>, _>("active_to_block_hash")?,
            Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned())
        );

        let active_to_contract_instance_id = query_scalar::<_, Uuid>(
            "SELECT to_contract_instance_id FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?;
        let reassigned_contract_instance_id = load_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000dd",
        )
        .await?;
        assert_eq!(
            active_to_contract_instance_id,
            reassigned_contract_instance_id
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000000000000000000000000000000dd".to_owned(),
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_ens_v1_subregistry_discovery_respects_manifest_discovery_rules() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(false))?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        insert_raw_new_owner_log(
            database.pool(),
            "ethereum-mainnet",
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            45,
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x00000000000000000000000000000000000000CC",
            CanonicalityState::Canonical,
        )
        .await?;

        let summary =
            sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
        assert_eq!(summary.scanned_log_count, 1);
        assert_eq!(summary.matched_log_count, 1);
        assert_eq!(summary.active_observation_count, 1);
        assert_eq!(summary.active_edge_count, 0);
        assert_eq!(summary.admitted_edge_count, 0);
        assert_eq!(summary.inserted_edge_count, 0);
        assert!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .is_empty()
        );

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
            }]
        );

        database.cleanup().await
    }
}
