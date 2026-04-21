use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContractSource, load_watched_contracts};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::json;
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Row};

const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
const SOURCE_FAMILY_BASENAMES_BASE_PRIMARY: &str = "basenames_base_primary";
const SOURCE_EVENT_REVERSE_CLAIMED: &str = "ReverseClaimed";
const DERIVATION_KIND_ENS_V1_REVERSE_CLAIM: &str = "ens_v1_reverse_claim";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const ENS_NATIVE_COIN_TYPE: &str = "60";
const CONTRACT_ROLE_REVERSE_REGISTRAR: &str = "reverse_registrar";
const REVERSE_CLAIMED_SIGNATURE: &str = "ReverseClaimed(address,bytes32)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1ReverseClaimSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV1ReverseClaimKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1ReverseClaimKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV1ReverseClaimSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_reverse_claim_with_scope(pool, chain, true, block_hashes).await
    }
}

#[derive(Clone, Debug)]
struct ReverseRawLogRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: sqlx::types::Uuid,
    topics: Vec<String>,
    canonicality_state: CanonicalityState,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
    contract_instance_id: sqlx::types::Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    source_rank: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveManifestMetadata {
    manifest_id: i64,
    chain: String,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

pub async fn sync_ens_v1_reverse_claim(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1ReverseClaimSyncSummary> {
    sync_ens_v1_reverse_claim_with_scope(pool, chain, false, &[]).await
}

async fn sync_ens_v1_reverse_claim_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV1ReverseClaimSyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV1ReverseClaimSyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let raw_logs = load_reverse_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV1ReverseClaimSyncSummary {
            scanned_log_count,
            matched_log_count: 0,
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let mut matched_log_refs = HashSet::new();
    let mut events = Vec::new();
    for raw_log in &raw_logs {
        let Some(event) = build_reverse_changed_event(raw_log)? else {
            continue;
        };
        matched_log_refs.insert((
            raw_log.chain_id.clone(),
            raw_log.block_hash.clone(),
            raw_log.transaction_hash.clone(),
            raw_log.log_index,
        ));
        events.push(event);
    }

    if events.is_empty() {
        return Ok(EnsV1ReverseClaimSyncSummary {
            scanned_log_count,
            matched_log_count: 0,
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let existing_event_identities = load_existing_event_identities(pool, &events).await?;
    let inserted_by_kind = count_inserted_events_by_kind(&events, &existing_event_identities);
    let synced_by_kind = count_events_by_kind(&events);

    upsert_normalized_events(pool, &events).await?;

    let by_kind = synced_by_kind
        .into_iter()
        .map(|(event_kind, synced_count)| {
            let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
            (
                event_kind,
                EnsV1ReverseClaimKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(EnsV1ReverseClaimSyncSummary {
        scanned_log_count,
        matched_log_count: matched_log_refs.len(),
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    })
}

fn build_reverse_changed_event(raw_log: &ReverseRawLogRow) -> Result<Option<NormalizedEvent>> {
    if !supports_reverse_claim_source_family(&raw_log.source_family) {
        return Ok(None);
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    if !topic0.eq_ignore_ascii_case(&reverse_claimed_topic0()) {
        return Ok(None);
    }

    let claimed_address = normalize_topic_address(
        raw_log
            .topics
            .get(1)
            .context("ReverseClaimed log is missing indexed address")?,
    )?;
    let indexed_reverse_node = normalize_hex_32(
        raw_log
            .topics
            .get(2)
            .context("ReverseClaimed log is missing indexed reverse node")?,
    )?;
    let reverse_label = reverse_label_for_address(&claimed_address)?;
    let reverse_name = format!("{reverse_label}.addr.reverse");
    let derived_reverse_node = reverse_node_for_address(&claimed_address)?;
    if !indexed_reverse_node.eq_ignore_ascii_case(&derived_reverse_node) {
        bail!(
            "ReverseClaimed indexed reverse node {} does not match derived reverse node {} for chain {} block {} log {}",
            indexed_reverse_node,
            derived_reverse_node,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(Some(NormalizedEvent {
        event_identity: format!(
            "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_REVERSE_CHANGED}:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            claimed_address
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": raw_log.chain_id,
            "block_hash": raw_log.block_hash,
            "block_number": raw_log.block_number,
            "transaction_hash": raw_log.transaction_hash,
            "transaction_index": raw_log.transaction_index,
            "log_index": raw_log.log_index,
            "emitting_address": raw_log.emitting_address,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": SOURCE_EVENT_REVERSE_CLAIMED,
            "address": claimed_address,
            "coin_type": ENS_NATIVE_COIN_TYPE,
            "namespace": raw_log.namespace,
            "reverse_namespace": raw_log.namespace,
            "reverse_label": reverse_label,
            "reverse_name": reverse_name,
            "reverse_node": derived_reverse_node,
            "claim_provenance": {
                "source_family": raw_log.source_family,
                "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
                "contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                "emitting_address": raw_log.emitting_address,
            },
        }),
    }))
}

fn supports_reverse_claim_source_family(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 | SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    )
}

async fn load_reverse_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<ReverseRawLogRow>> {
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
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        WHERE rl.chain_id = $1
          AND lower(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
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
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv1 reverse raw logs for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?
                .to_ascii_lowercase();
            let emitter = emitters_by_address.get(&address).with_context(|| {
                format!("missing active emitter metadata for chain {chain} address {address}")
            })?;

            Ok(ReverseRawLogRow {
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
                emitting_address: address,
                emitting_contract_instance_id: emitter.contract_instance_id,
                topics: row.try_get("topics").context("missing topics")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
            })
        })
        .collect()
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv1 reverse attribution")?;
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
        if manifest.chain != watched_contract.chain {
            bail!(
                "watched contract chain {} does not match active manifest chain {} for manifest_id {}",
                watched_contract.chain,
                manifest.chain,
                source_manifest_id
            );
        }
        if !supports_reverse_claim_source_family(&manifest.source_family) {
            continue;
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
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
        SELECT manifest_id, chain, namespace, source_family, manifest_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 reverse")?;

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

async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();

    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(event_identities)
    .fetch_all(pool)
    .await
    .context("failed to load existing ENSv1 reverse normalized-event identities")?;

    Ok(rows.into_iter().collect())
}

fn count_inserted_events_by_kind(
    events: &[NormalizedEvent],
    existing_event_identities: &HashSet<String>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events
        .iter()
        .filter(|event| !existing_event_identities.contains(&event.event_identity))
    {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if !normalized.starts_with("0x") || normalized.len() != 42 {
        bail!("expected 20-byte address, got {value}");
    }
    Ok(normalized)
}

fn reverse_label_for_address(address: &str) -> Result<String> {
    Ok(normalize_address(address)?
        .trim_start_matches("0x")
        .to_owned())
}

fn reverse_node_for_address(address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    Ok(namehash_hex(&[
        reverse_label.into_bytes(),
        b"addr".to_vec(),
        b"reverse".to_vec(),
    ]))
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

fn reverse_claimed_topic0() -> String {
    keccak256_hex(REVERSE_CLAIMED_SIGNATURE.as_bytes())
}

fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut digest = Keccak256::new();
            digest.update(label);
            let output = digest.finalize();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&output);
            bytes
        };
        let mut digest = Keccak256::new();
        digest.update(node);
        digest.update(label_hash);
        let output = digest.finalize();
        node.copy_from_slice(&output);
    }

    hex_string(&node)
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let mut digest = Keccak256::new();
    digest.update(bytes);
    hex_string(&digest.finalize())
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

    use super::*;
    use bigname_storage::{
        MIGRATOR, RawBlock, RawLog, default_database_url, load_normalized_event_counts_by_kind,
        load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::{Uuid, time::OffsetDateTime},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    struct TestReverseClaimConfig<'a> {
        namespace: &'a str,
        source_family: &'a str,
        chain: &'a str,
        deployment_epoch: &'a str,
        file_path: &'a str,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for ENSv1 reverse tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_ens_v1_reverse_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for ENSv1 reverse tests")?;
            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for ENSv1 reverse tests")?;
            MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for ENSv1 reverse tests")?;

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
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6::manifest_rollout_status,
                'uts46-v1',
                $7,
                '{}'::jsonb
            )
            RETURNING manifest_id
            "#,
        )
        .bind(manifest_version)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(file_path)
        .fetch_one(pool)
        .await
        .context("failed to insert manifest version")
    }

    async fn insert_contract_instance(
        pool: &PgPool,
        chain: &str,
        contract_instance_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, 'contract', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain)
        .execute(pool)
        .await
        .context("failed to insert contract instance")?;
        Ok(())
    }

    async fn insert_manifest_contract_instance(
        pool: &PgPool,
        manifest_id: i64,
        contract_instance_id: Uuid,
        address: &str,
    ) -> Result<()> {
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
            VALUES ($1, 'contract', 'reverse_registrar', $2, $3, 'reverse_registrar', 'none')
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert manifest reverse contract instance")?;
        Ok(())
    }

    async fn insert_contract_instance_address(
        pool: &PgPool,
        chain: &str,
        contract_instance_id: Uuid,
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
            VALUES ($1, $2, $3, $4, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain)
        .bind(address)
        .bind(source_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert contract-instance address")?;
        Ok(())
    }

    async fn insert_raw_reverse_claim_log(
        pool: &PgPool,
        chain: &str,
        block_hash: &str,
        block_number: i64,
        emitting_address: &str,
        claimed_address: &str,
        canonicality_state: CanonicalityState,
    ) -> Result<()> {
        upsert_raw_blocks(
            pool,
            &[RawBlock {
                chain_id: chain.to_owned(),
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
                chain_id: chain.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: format!("0xtx{block_number:02x}"),
                transaction_index: 0,
                log_index: 0,
                emitting_address: emitting_address.to_owned(),
                topics: vec![
                    reverse_claimed_topic0(),
                    hex_string(&abi_word_address(claimed_address)),
                    reverse_node_for_address(claimed_address)?,
                ],
                data: Vec::new(),
                canonicality_state,
            }],
        )
        .await?;
        Ok(())
    }

    async fn run_idempotence_case(config: TestReverseClaimConfig<'_>) -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            config.namespace,
            config.source_family,
            config.chain,
            config.deployment_epoch,
            "active",
            config.file_path,
        )
        .await?;
        let draft_manifest_id = insert_manifest_version(
            database.pool(),
            2,
            config.namespace,
            config.source_family,
            config.chain,
            config.deployment_epoch,
            "draft",
            "manifests/test/draft.toml",
        )
        .await?;
        let active_contract_instance_id = Uuid::new_v4();
        let draft_contract_instance_id = Uuid::new_v4();
        let active_emitter = "0x00000000000000000000000000000000000000aa";
        let draft_emitter = "0x00000000000000000000000000000000000000bb";
        let claimed_address = "0x1111111111111111111111111111111111111111";

        insert_contract_instance(database.pool(), config.chain, active_contract_instance_id)
            .await?;
        insert_contract_instance(database.pool(), config.chain, draft_contract_instance_id).await?;
        insert_manifest_contract_instance(
            database.pool(),
            active_manifest_id,
            active_contract_instance_id,
            active_emitter,
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            draft_manifest_id,
            draft_contract_instance_id,
            draft_emitter,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            config.chain,
            active_contract_instance_id,
            active_emitter,
            active_manifest_id,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            config.chain,
            draft_contract_instance_id,
            draft_emitter,
            draft_manifest_id,
        )
        .await?;

        insert_raw_reverse_claim_log(
            database.pool(),
            config.chain,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            active_emitter,
            claimed_address,
            CanonicalityState::Canonical,
        )
        .await?;
        insert_raw_reverse_claim_log(
            database.pool(),
            config.chain,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            43,
            draft_emitter,
            "0x2222222222222222222222222222222222222222",
            CanonicalityState::Canonical,
        )
        .await?;

        let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
        assert_eq!(first.scanned_log_count, 1);
        assert_eq!(first.matched_log_count, 1);
        assert_eq!(first.total_synced_count, 1);
        assert_eq!(first.total_inserted_count, 1);
        assert_eq!(
            first.by_kind,
            BTreeMap::from([(
                EVENT_KIND_REVERSE_CHANGED.to_owned(),
                EnsV1ReverseClaimKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                }
            )])
        );

        let events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_kind, EVENT_KIND_REVERSE_CHANGED);
        assert_eq!(
            events[0].derivation_kind,
            DERIVATION_KIND_ENS_V1_REVERSE_CLAIM
        );
        assert_eq!(events[0].source_family, config.source_family);
        assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
        assert_eq!(events[0].chain_id.as_deref(), Some(config.chain));
        assert_eq!(
            events[0].after_state["address"],
            claimed_address.to_ascii_lowercase()
        );
        assert_eq!(events[0].after_state["coin_type"], ENS_NATIVE_COIN_TYPE);
        assert_eq!(events[0].after_state["namespace"], config.namespace);
        assert_eq!(events[0].after_state["reverse_namespace"], config.namespace);
        assert_eq!(
            events[0].after_state["reverse_node"],
            reverse_node_for_address(claimed_address)?
        );
        assert_eq!(
            events[0].after_state["reverse_name"],
            format!(
                "{}.addr.reverse",
                claimed_address
                    .trim_start_matches("0x")
                    .to_ascii_lowercase()
            )
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["source_family"],
            config.source_family
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["contract_role"],
            CONTRACT_ROLE_REVERSE_REGISTRAR
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["contract_instance_id"],
            active_contract_instance_id.to_string()
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["emitting_address"],
            active_emitter
        );

        let second = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
        assert_eq!(second.scanned_log_count, 1);
        assert_eq!(second.matched_log_count, 1);
        assert_eq!(second.total_synced_count, 1);
        assert_eq!(second.total_inserted_count, 0);

        let counts =
            load_normalized_event_counts_by_kind(database.pool(), config.namespace).await?;
        assert_eq!(
            counts,
            BTreeMap::from([(EVENT_KIND_REVERSE_CHANGED.to_owned(), 1_usize)])
        );

        database.cleanup().await
    }

    async fn run_canonicality_case(config: TestReverseClaimConfig<'_>) -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let manifest_id = insert_manifest_version(
            database.pool(),
            1,
            config.namespace,
            config.source_family,
            config.chain,
            config.deployment_epoch,
            "active",
            config.file_path,
        )
        .await?;
        let contract_instance_id = Uuid::new_v4();
        let emitter = "0x00000000000000000000000000000000000000aa";
        let claimed_address = "0x3333333333333333333333333333333333333333";

        insert_contract_instance(database.pool(), config.chain, contract_instance_id).await?;
        insert_manifest_contract_instance(
            database.pool(),
            manifest_id,
            contract_instance_id,
            emitter,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            config.chain,
            contract_instance_id,
            emitter,
            manifest_id,
        )
        .await?;

        insert_raw_reverse_claim_log(
            database.pool(),
            config.chain,
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            44,
            emitter,
            claimed_address,
            CanonicalityState::Safe,
        )
        .await?;

        let first = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
        assert_eq!(first.total_inserted_count, 1);
        let mut events =
            load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].canonicality_state, CanonicalityState::Safe);

        insert_raw_reverse_claim_log(
            database.pool(),
            config.chain,
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            44,
            emitter,
            claimed_address,
            CanonicalityState::Finalized,
        )
        .await?;

        let second = sync_ens_v1_reverse_claim(database.pool(), config.chain).await?;
        assert_eq!(second.total_inserted_count, 0);
        events = load_normalized_events_by_namespace(database.pool(), config.namespace).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].canonicality_state, CanonicalityState::Finalized);
        assert_eq!(events[0].source_family, config.source_family);
        assert_eq!(events[0].chain_id.as_deref(), Some(config.chain));
        assert_eq!(events[0].after_state["namespace"], config.namespace);
        assert_eq!(events[0].after_state["reverse_namespace"], config.namespace);
        assert_eq!(
            events[0].after_state["claim_provenance"]["contract_role"],
            CONTRACT_ROLE_REVERSE_REGISTRAR
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["contract_instance_id"],
            contract_instance_id.to_string()
        );
        assert_eq!(
            events[0].after_state["claim_provenance"]["emitting_address"],
            emitter
        );

        database.cleanup().await
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
    async fn sync_ens_v1_reverse_claim_is_idempotent() -> Result<()> {
        run_idempotence_case(TestReverseClaimConfig {
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            file_path: "manifests/ens/ens_v1_reverse_l1/v1.toml",
        })
        .await
    }

    #[tokio::test]
    async fn sync_ens_v1_reverse_claim_is_idempotent_for_basenames_base_primary() -> Result<()> {
        run_idempotence_case(TestReverseClaimConfig {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            file_path: "manifests/basenames/basenames_base_primary/v1.toml",
        })
        .await
    }

    #[tokio::test]
    async fn sync_ens_v1_reverse_claim_updates_event_canonicality() -> Result<()> {
        run_canonicality_case(TestReverseClaimConfig {
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REVERSE_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            file_path: "manifests/ens/ens_v1_reverse_l1/v1.toml",
        })
        .await
    }

    #[tokio::test]
    async fn sync_ens_v1_reverse_claim_updates_basenames_event_canonicality() -> Result<()> {
        run_canonicality_case(TestReverseClaimConfig {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            file_path: "manifests/basenames/basenames_base_primary/v1.toml",
        })
        .await
    }
}
