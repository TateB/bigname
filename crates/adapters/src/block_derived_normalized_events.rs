use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContractSource, load_watched_contracts};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::json;
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Row};

const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str = "raw_log_preimage_observation";
const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
const NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes,bytes32,address,uint32,uint64)";

/// Sync summary for block-derived normalized events rebuilt from persisted raw payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDerivedNormalizedEventSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, BlockDerivedNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDerivedNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
struct WatchedRawLogRow {
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

#[derive(Clone, Debug)]
struct ActiveManifestMetadata {
    manifest_id: i64,
    chain: String,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug)]
struct PreimageObservation {
    dns_encoded_name: String,
    decoded_name: Option<String>,
    labelhashes: Vec<String>,
    namehash: String,
}

/// Sync the first block-derived normalized events from stored raw logs.
pub async fn sync_block_derived_normalized_events(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<BlockDerivedNormalizedEventSyncSummary> {
    if block_hashes.is_empty() {
        return Ok(BlockDerivedNormalizedEventSyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let scanned_log_count = load_scanned_log_count(pool, chain, block_hashes).await?;
    let raw_logs = load_watched_raw_logs(pool, chain, block_hashes).await?;
    if raw_logs.is_empty() {
        return Ok(BlockDerivedNormalizedEventSyncSummary {
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
        let Some(event) = build_preimage_observed_event(raw_log)? else {
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
        return Ok(BlockDerivedNormalizedEventSyncSummary {
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
                BlockDerivedNormalizedEventKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(BlockDerivedNormalizedEventSyncSummary {
        scanned_log_count,
        matched_log_count: matched_log_refs.len(),
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    })
}

fn build_preimage_observed_event(raw_log: &WatchedRawLogRow) -> Result<Option<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    if !topic0.eq_ignore_ascii_case(&name_wrapped_topic0()) {
        return Ok(None);
    }

    let dns_name = decode_first_dynamic_bytes(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NameWrapped bytes payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation = observe_dns_encoded_name(&dns_name).with_context(|| {
        format!(
            "failed to interpret dns-encoded name for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    if let Some(indexed_namehash) = raw_log.topics.get(1)
        && !indexed_namehash.eq_ignore_ascii_case(&observation.namehash)
    {
        bail!(
            "NameWrapped indexed namehash {} does not match decoded namehash {} for chain {} block {} log {}",
            indexed_namehash,
            observation.namehash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(Some(NormalizedEvent {
        event_identity: format!(
            "raw_log_preimage_observed:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            raw_log.emitting_address
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PREIMAGE_OBSERVED.to_owned(),
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
            "chain_id": raw_log.chain_id.clone(),
            "block_hash": raw_log.block_hash.clone(),
            "block_number": raw_log.block_number,
            "transaction_hash": raw_log.transaction_hash.clone(),
            "transaction_index": raw_log.transaction_index,
            "log_index": raw_log.log_index,
            "emitting_address": raw_log.emitting_address.clone(),
            "topic0": raw_log.topics.first().cloned(),
            "topic1": raw_log.topics.get(1).cloned(),
            "data_hex": hex_string(&raw_log.data),
        }),
        derivation_kind: DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "NameWrapped",
            "dns_encoded_name": observation.dns_encoded_name,
            "decoded_name": observation.decoded_name,
            "labelhashes": observation.labelhashes,
            "namehash": observation.namehash,
        }),
    }))
}

async fn load_scanned_log_count(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count stored raw logs for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    usize::try_from(count).context("raw log count does not fit in usize")
}

async fn load_watched_raw_logs(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<Vec<WatchedRawLogRow>> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(Vec::new());
    }

    let emitters_by_address = active_emitters
        .into_iter()
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
            rl.data AS data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        WHERE rl.chain_id = $1
          AND rl.block_hash = ANY($2::TEXT[])
          AND lower(rl.emitting_address) = ANY($3::TEXT[])
          AND rl.canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY
            rl.block_number,
            rl.transaction_index,
            rl.log_index
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(&watched_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load watched raw logs for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let emitting_address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?;
            let normalized_emitting_address = emitting_address.to_ascii_lowercase();
            let active_emitter = emitters_by_address
                .get(&normalized_emitting_address)
                .with_context(|| {
                    format!(
                        "missing active emitter attribution for chain {} address {}",
                        chain, emitting_address
                    )
                })?;

            Ok(WatchedRawLogRow {
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
                source_manifest_id: active_emitter.source_manifest_id,
                namespace: active_emitter.namespace.clone(),
                source_family: active_emitter.source_family.clone(),
                manifest_version: active_emitter.manifest_version,
            })
        })
        .collect()
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for adapter emitter attribution")?;
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
    .context("failed to load active manifest metadata for watched contracts")?;

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
    .context("failed to load existing block-derived normalized-event identities")?;

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

fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 64 {
        bail!("event data is too short to decode a dynamic bytes parameter");
    }

    let offset = word_to_usize(&data[0..32]).context("invalid ABI offset for dynamic bytes")?;
    if data.len() < offset + 32 {
        bail!("event data does not contain the dynamic bytes length word");
    }
    let byte_length = word_to_usize(&data[offset..offset + 32])
        .context("invalid ABI length for dynamic bytes")?;
    let bytes_start = offset + 32;
    let bytes_end = bytes_start + byte_length;
    if data.len() < bytes_end {
        bail!("event data does not contain the full dynamic bytes payload");
    }

    Ok(data[bytes_start..bytes_end].to_vec())
}

fn observe_dns_encoded_name(bytes: &[u8]) -> Result<PreimageObservation> {
    if bytes.is_empty() {
        bail!("dns-encoded name payload must not be empty");
    }

    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            bail!("dns-encoded name payload is missing the root terminator");
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                bail!("dns-encoded name payload has trailing bytes after the root terminator");
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            bail!("dns-encoded name label exceeds the available payload");
        }
        labels.push(bytes[cursor..cursor + label_length].to_vec());
        cursor += label_length;
    }

    let decoded_labels = labels
        .iter()
        .map(|label| String::from_utf8(label.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok();
    let labelhashes = labels
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();
    let namehash = namehash_hex(&labels);

    Ok(PreimageObservation {
        dns_encoded_name: hex_string(bytes),
        decoded_name: decoded_labels.map(|labels| labels.join(".")),
        labelhashes,
        namehash,
    })
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
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

fn name_wrapped_topic0() -> String {
    keccak256_hex(NAME_WRAPPED_SIGNATURE.as_bytes())
}

fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    hex_string(&node)
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(&keccak256_bytes(bytes))
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

    use anyhow::Result;
    use bigname_storage::{
        RawBlock, RawLog, default_database_url, load_normalized_event_counts_by_kind,
        load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };
    use uuid::Uuid;

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
                .context("failed to parse database URL for block-derived normalized-event tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_block_derived_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for block-derived normalized-event tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for block-derived normalized-event tests")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for block-derived normalized-event tests")?;

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
        implementation_contract_instance_id: Option<Uuid>,
        declared_implementation_address: Option<&str>,
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
            VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, $8, $9)
            "#,
        )
        .bind(manifest_id)
        .bind(declaration_kind)
        .bind(declaration_name)
        .bind(contract_instance_id)
        .bind(declared_address)
        .bind(role)
        .bind(proxy_kind)
        .bind(implementation_contract_instance_id)
        .bind(declared_implementation_address)
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

    async fn deactivate_active_contract_instance_addresses(
        pool: &PgPool,
        contract_instance_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET deactivated_at = now()
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to deactivate contract-instance address rows")?;
        Ok(())
    }

    async fn insert_discovery_edge(
        pool: &PgPool,
        chain_id: &str,
        edge_kind: &str,
        from_contract_instance_id: Uuid,
        to_contract_instance_id: Uuid,
        source_manifest_id: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            "#,
        )
        .bind(chain_id)
        .bind(edge_kind)
        .bind(from_contract_instance_id)
        .bind(to_contract_instance_id)
        .bind(format!("test:{edge_kind}"))
        .bind(source_manifest_id)
        .bind("automatic")
        .bind("{}")
        .execute(pool)
        .await
        .context("failed to insert discovery edge")?;
        Ok(())
    }

    async fn insert_raw_name_wrapped_log(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        address: &str,
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

        let dns_name = dns_encoded_name(&["wrapped", "eth"]);
        upsert_raw_logs(
            pool,
            &[RawLog {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                transaction_hash: format!("0xtx{block_number:02x}"),
                transaction_index: 0,
                log_index: 0,
                emitting_address: address.to_owned(),
                topics: vec![name_wrapped_topic0(), namehash_hex_bytes(&dns_name)],
                data: encode_name_wrapped_log_data(&dns_name),
                canonicality_state,
            }],
        )
        .await?;

        Ok(())
    }

    fn dns_encoded_name(labels: &[&str]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for label in labels {
            encoded.push(u8::try_from(label.len()).expect("test label length must fit in u8"));
            encoded.extend_from_slice(label.as_bytes());
        }
        encoded.push(0);
        encoded
    }

    fn namehash_hex_bytes(dns_name: &[u8]) -> String {
        let observation =
            observe_dns_encoded_name(dns_name).expect("test dns-encoded name must decode");
        observation.namehash
    }

    fn encode_name_wrapped_log_data(dns_name: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();

        output.extend_from_slice(&abi_word_u64(128));
        output.extend_from_slice(&abi_word_address(
            "0x0000000000000000000000000000000000000001",
        ));
        output.extend_from_slice(&abi_word_u64(0));
        output.extend_from_slice(&abi_word_u64(0));
        output.extend_from_slice(&abi_word_u64(
            u64::try_from(dns_name.len()).expect("test dns-encoded name length must fit in u64"),
        ));
        output.extend_from_slice(dns_name);

        let padded_length = ((dns_name.len() + 31) / 32) * 32;
        output.resize(32 * 5 + padded_length, 0);
        output
    }

    fn abi_word_u64(value: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
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
    async fn sync_block_derived_normalized_events_is_idempotent() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v1_name_wrapper",
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_name_wrapper/1.toml",
        )
        .await?;
        let inactive_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v1_name_wrapper",
            "ethereum-mainnet",
            "ens_v1_shadow",
            "draft",
            "uts46-v1",
            "manifests/ens/ens_v1_name_wrapper/2.toml",
        )
        .await?;

        let active_contract_instance_id = Uuid::new_v4();
        let inactive_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            active_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            inactive_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;

        insert_manifest_contract_instance(
            database.pool(),
            active_manifest_id,
            "contract",
            "wrapper",
            active_contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("wrapper"),
            Some("none"),
            None,
            None,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            active_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            active_manifest_id,
        )
        .await?;

        insert_manifest_contract_instance(
            database.pool(),
            inactive_manifest_id,
            "contract",
            "wrapper",
            inactive_contract_instance_id,
            "0x00000000000000000000000000000000000000bb",
            Some("wrapper"),
            Some("none"),
            None,
            None,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            inactive_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000bb",
            inactive_manifest_id,
        )
        .await?;

        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "0x00000000000000000000000000000000000000aa",
            CanonicalityState::Canonical,
        )
        .await?;
        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            41,
            "0x00000000000000000000000000000000000000bb",
            CanonicalityState::Canonical,
        )
        .await?;

        let first = sync_block_derived_normalized_events(
            database.pool(),
            "ethereum-mainnet",
            &[
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ],
        )
        .await?;
        assert_eq!(first.scanned_log_count, 2);
        assert_eq!(first.matched_log_count, 1);
        assert_eq!(first.total_synced_count, 1);
        assert_eq!(first.total_inserted_count, 1);
        assert_eq!(
            first.by_kind,
            BTreeMap::from([(
                EVENT_KIND_PREIMAGE_OBSERVED.to_owned(),
                BlockDerivedNormalizedEventKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                }
            )])
        );

        let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_kind, EVENT_KIND_PREIMAGE_OBSERVED);
        assert_eq!(
            events[0].derivation_kind,
            DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION
        );
        assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
        assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
        assert_eq!(events[0].after_state["decoded_name"], "wrapped.eth");

        let second = sync_block_derived_normalized_events(
            database.pool(),
            "ethereum-mainnet",
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        )
        .await?;
        assert_eq!(second.scanned_log_count, 1);
        assert_eq!(second.matched_log_count, 1);
        assert_eq!(second.total_synced_count, 1);
        assert_eq!(second.total_inserted_count, 0);

        let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
        assert_eq!(
            counts,
            BTreeMap::from([(EVENT_KIND_PREIMAGE_OBSERVED.to_owned(), 1_usize)])
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_block_derived_normalized_events_uses_active_manifest_after_reactivation_gap()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let previous_manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v1_name_wrapper",
            "ethereum-mainnet",
            "ens_v0",
            "deprecated",
            "uts46-v1",
            "manifests/ens/ens_v1_name_wrapper/0.toml",
        )
        .await?;
        let active_manifest_id = insert_manifest_version(
            database.pool(),
            2,
            "ens",
            "ens_v1_name_wrapper",
            "ethereum-mainnet",
            "ens_v1",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v1_name_wrapper/1.toml",
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
            active_manifest_id,
            "contract",
            "wrapper",
            contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("wrapper"),
            Some("none"),
            None,
            None,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            previous_manifest_id,
        )
        .await?;
        deactivate_active_contract_instance_addresses(database.pool(), contract_instance_id)
            .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            active_manifest_id,
        )
        .await?;
        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "0x00000000000000000000000000000000000000aa",
            CanonicalityState::Canonical,
        )
        .await?;

        let first = sync_block_derived_normalized_events(
            database.pool(),
            "ethereum-mainnet",
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        )
        .await?;
        assert_eq!(first.scanned_log_count, 1);
        assert_eq!(first.matched_log_count, 1);
        assert_eq!(first.total_synced_count, 1);
        assert_eq!(first.total_inserted_count, 1);

        let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
        assert_eq!(events[0].manifest_version, 2);
        assert_eq!(
            events[0].raw_fact_ref["emitting_address"],
            "0x00000000000000000000000000000000000000aa"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_block_derived_normalized_events_watches_proxy_implementations_but_not_migrations()
    -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            "active",
            "uts46-v1",
            "manifests/ens/ens_v2_registry_l1/1.toml",
        )
        .await?;
        let proxy_contract_instance_id = Uuid::new_v4();
        let implementation_contract_instance_id = Uuid::new_v4();
        let successor_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            proxy_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            implementation_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            successor_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;

        insert_manifest_contract_instance(
            database.pool(),
            manifest_id,
            "contract",
            "registry",
            proxy_contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("registry"),
            Some("erc1967"),
            Some(implementation_contract_instance_id),
            Some("0x00000000000000000000000000000000000000dd"),
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            proxy_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
            manifest_id,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            implementation_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000dd",
            manifest_id,
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            successor_contract_instance_id,
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000ee",
            manifest_id,
        )
        .await?;
        insert_discovery_edge(
            database.pool(),
            "ethereum-mainnet",
            "proxy_implementation",
            proxy_contract_instance_id,
            implementation_contract_instance_id,
            manifest_id,
        )
        .await?;
        insert_discovery_edge(
            database.pool(),
            "ethereum-mainnet",
            "migration",
            proxy_contract_instance_id,
            successor_contract_instance_id,
            manifest_id,
        )
        .await?;

        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            43,
            "0x00000000000000000000000000000000000000dd",
            CanonicalityState::Canonical,
        )
        .await?;
        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            44,
            "0x00000000000000000000000000000000000000ee",
            CanonicalityState::Canonical,
        )
        .await?;

        let summary = sync_block_derived_normalized_events(
            database.pool(),
            "ethereum-mainnet",
            &[
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            ],
        )
        .await?;
        assert_eq!(summary.scanned_log_count, 2);
        assert_eq!(summary.matched_log_count, 1);
        assert_eq!(summary.total_synced_count, 1);
        assert_eq!(summary.total_inserted_count, 1);

        let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_manifest_id, Some(manifest_id));
        assert_eq!(
            events[0].raw_fact_ref["emitting_address"],
            "0x00000000000000000000000000000000000000dd"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_block_derived_normalized_events_skips_inactive_manifests() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let manifest_id = insert_manifest_version(
            database.pool(),
            1,
            "ens",
            "ens_v1_name_wrapper",
            "ethereum-mainnet",
            "ens_v1",
            "deprecated",
            "uts46-v1",
            "manifests/ens/ens_v1_name_wrapper/1.toml",
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
            "wrapper",
            contract_instance_id,
            "0x00000000000000000000000000000000000000aa",
            Some("wrapper"),
            Some("none"),
            None,
            None,
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
        insert_raw_name_wrapped_log(
            database.pool(),
            "ethereum-mainnet",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "0x00000000000000000000000000000000000000aa",
            CanonicalityState::Canonical,
        )
        .await?;

        let summary = sync_block_derived_normalized_events(
            database.pool(),
            "ethereum-mainnet",
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        )
        .await?;
        assert_eq!(summary.scanned_log_count, 1);
        assert_eq!(summary.matched_log_count, 0);
        assert_eq!(summary.total_synced_count, 0);
        assert_eq!(summary.total_inserted_count, 0);
        assert!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .is_empty()
        );

        database.cleanup().await
    }
}
