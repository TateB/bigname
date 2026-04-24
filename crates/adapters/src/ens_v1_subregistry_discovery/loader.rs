use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use bigname_storage::CanonicalityState;
use sqlx::{PgPool, Row, types::Uuid};

use super::{
    BASENAMES_BASE_REGISTRY_SOURCE_FAMILY, ENS_V1_REGISTRY_SOURCE_FAMILY, SUBREGISTRY_EDGE_KIND,
    hex_topic::normalize_address,
};

#[derive(Clone, Debug)]
pub(super) struct RegistryRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    address: String,
    contract_instance_id: Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    source_rank: i32,
}

#[derive(Clone, Debug)]
pub(super) struct ActiveRegistryEdge {
    pub(super) observation_key: String,
    pub(super) discovery_source: String,
    pub(super) from_contract_instance_id: Uuid,
    pub(super) to_contract_instance_id: Uuid,
}

pub(super) async fn load_registry_raw_logs(
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

pub(super) async fn load_active_registry_edges_by_observation_key(
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

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
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
