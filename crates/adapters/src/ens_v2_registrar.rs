use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContractSource, load_watched_contracts};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Row, types::Uuid};

const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
const DERIVATION_KIND_ENS_V2_REGISTRAR: &str = "ens_v2_registrar";
const REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const EVENT_KIND_REGISTRAR_NAME_REGISTERED: &str = "RegistrarNameRegistered";
const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";

const NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)";
const NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2RegistrarKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV2RegistrarSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registrar_with_scope(pool, chain, true, block_hashes).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistrarRawLogRow {
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
struct ResourceLink {
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
}

enum RegistrarObservation {
    NameRegistered {
        token_id: String,
        label: String,
        owner: String,
        subregistry: String,
        resolver: String,
        duration: i64,
        payment_token: String,
        referrer: String,
        base: String,
        premium: String,
    },
    NameRenewed {
        token_id: String,
        label: String,
        duration: i64,
        new_expiry: i64,
        payment_token: String,
        referrer: String,
        base: String,
    },
}

pub async fn sync_ens_v2_registrar(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistrarSyncSummary> {
    sync_ens_v2_registrar_with_scope(pool, chain, false, &[]).await
}

async fn sync_ens_v2_registrar_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV2RegistrarSyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(empty_summary(0));
    }

    let raw_logs = load_registrar_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let mut matched_log_count = 0usize;
    let mut events = Vec::new();
    for raw_log in &raw_logs {
        let Some(observation) = build_registrar_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;
        events.push(build_registrar_event(pool, raw_log, observation).await?);
    }

    let existing = load_existing_event_identities(pool, &events).await?;
    let inserted_by_kind = count_inserted_events_by_kind(&events, &existing);
    let synced_by_kind = count_events_by_kind(&events);
    upsert_normalized_events(pool, &events).await?;

    let by_kind = synced_by_kind
        .into_iter()
        .map(|(event_kind, synced_count)| {
            let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
            (
                event_kind,
                EnsV2RegistrarKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(EnsV2RegistrarSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    })
}

async fn build_registrar_event(
    pool: &PgPool,
    raw_log: &RegistrarRawLogRow,
    observation: RegistrarObservation,
) -> Result<NormalizedEvent> {
    let (event_kind, token_id, label, after_state) = match observation {
        RegistrarObservation::NameRegistered {
            token_id,
            label,
            owner,
            subregistry,
            resolver,
            duration,
            payment_token,
            referrer,
            base,
            premium,
        } => (
            EVENT_KIND_REGISTRAR_NAME_REGISTERED,
            token_id,
            label,
            json!({
                "source_event": "NameRegistered",
                "owner": owner,
                "subregistry": null_if_zero_address(&subregistry),
                "resolver": null_if_zero_address(&resolver),
                "duration": duration,
                "payment_token": payment_token,
                "referrer": referrer,
                "base": base,
                "premium": premium,
            }),
        ),
        RegistrarObservation::NameRenewed {
            token_id,
            label,
            duration,
            new_expiry,
            payment_token,
            referrer,
            base,
        } => (
            EVENT_KIND_REGISTRATION_RENEWED,
            token_id,
            label,
            json!({
                "source_event": "NameRenewed",
                "duration": duration,
                "expiry": new_expiry,
                "payment_token": payment_token,
                "referrer": referrer,
                "base": base,
            }),
        ),
    };

    let link = load_registry_resource_link(pool, &raw_log.namespace, &token_id).await?;
    let logical_name_id = link.logical_name_id.or_else(|| {
        Some(format!(
            "{}:{}.eth",
            raw_log.namespace,
            label.to_ascii_lowercase()
        ))
    });
    let mut after_state = after_state;
    if let Some(object) = after_state.as_object_mut() {
        object.insert("token_id".to_owned(), Value::String(token_id.clone()));
        object.insert("label".to_owned(), Value::String(label));
        object.insert(
            "registry_resource_id".to_owned(),
            link.resource_id
                .map(|value| Value::String(value.to_string()))
                .unwrap_or(Value::Null),
        );
    }

    Ok(NormalizedEvent {
        event_identity: format!(
            "ens_v2_registrar:{}:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            event_kind,
            token_id
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id,
        resource_id: link.resource_id,
        event_kind: event_kind.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: raw_fact_ref(raw_log),
        derivation_kind: DERIVATION_KIND_ENS_V2_REGISTRAR.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    })
}

async fn load_registry_resource_link(
    pool: &PgPool,
    namespace: &str,
    token_id: &str,
) -> Result<ResourceLink> {
    let row = sqlx::query(
        r#"
        SELECT logical_name_id, resource_id
        FROM normalized_events
        WHERE namespace = $1
          AND derivation_kind = $2
          AND event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
          AND (
              after_state ->> 'token_id' = $3
              OR after_state ->> 'old_token_id' = $3
              OR after_state ->> 'new_token_id' = $3
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number DESC NULLS LAST, log_index DESC NULLS LAST, event_identity DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(REGISTRY_DERIVATION_KIND)
    .bind(token_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 registry resource link for token {token_id}"))?;

    Ok(match row {
        Some(row) => ResourceLink {
            logical_name_id: row
                .try_get("logical_name_id")
                .context("missing logical_name_id")?,
            resource_id: row.try_get("resource_id").context("missing resource_id")?,
        },
        None => ResourceLink {
            logical_name_id: None,
            resource_id: None,
        },
    })
}

fn build_registrar_observation(
    raw_log: &RegistrarRawLogRow,
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id,
            label: decode_dynamic_string(&raw_log.data, 0)?,
            owner: decode_address_word(&raw_log.data, 1)?,
            subregistry: decode_address_word(&raw_log.data, 2)?,
            resolver: decode_address_word(&raw_log.data, 3)?,
            duration: decode_u64_word(&raw_log.data, 4)?,
            payment_token: decode_address_word(&raw_log.data, 5)?,
            referrer: format!("0x{}", hex_string(word_at(&raw_log.data, 6)?)),
            base: normalize_word_hex(word_at(&raw_log.data, 7)?),
            premium: normalize_word_hex(word_at(&raw_log.data, 8)?),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_RENEWED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id,
            label: decode_dynamic_string(&raw_log.data, 0)?,
            duration: decode_u64_word(&raw_log.data, 1)?,
            new_expiry: decode_u64_word(&raw_log.data, 2)?,
            payment_token: decode_address_word(&raw_log.data, 3)?,
            referrer: format!("0x{}", hex_string(word_at(&raw_log.data, 4)?)),
            base: normalize_word_hex(word_at(&raw_log.data, 5)?),
        }));
    }

    Ok(None)
}

async fn load_registrar_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<RegistrarRawLogRow>> {
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
          AND ($3::BOOLEAN = FALSE OR block_hash = ANY($4::TEXT[]))
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
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 registrar raw logs for chain {chain}"))?;

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
                        "missing ENSv2 registrar emitter attribution for chain {chain} address {emitting_address}"
                    )
                })?;
            Ok(RegistrarRawLogRow {
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
        .context("failed to load watched contracts for ENSv2 registrar adapter")?;
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
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 {
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
    .context("failed to load active manifest metadata for ENSv2 registrar emitters")?;

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

async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query_scalar::<_, String>(
        "SELECT event_identity FROM normalized_events WHERE event_identity = ANY($1::TEXT[])",
    )
    .bind(event_identities)
    .fetch_all(pool)
    .await
    .context("failed to load existing ENSv2 registrar event identities")?;

    Ok(rows.into_iter().collect())
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
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

fn empty_summary(scanned_log_count: usize) -> EnsV2RegistrarSyncSummary {
    EnsV2RegistrarSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn raw_fact_ref(raw_log: &RegistrarRawLogRow) -> Value {
    json!({
        "kind": "raw_log",
        "chain_id": raw_log.chain_id,
        "block_hash": raw_log.block_hash,
        "block_number": raw_log.block_number,
        "transaction_hash": raw_log.transaction_hash,
        "transaction_index": raw_log.transaction_index,
        "log_index": raw_log.log_index,
        "emitting_address": raw_log.emitting_address,
        "topic0": raw_log.topics.first().cloned(),
        "data_hex": hex_string(&raw_log.data),
    })
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (candidate.source_rank, candidate.source_manifest_id)
        < (current.source_rank, current.source_manifest_id)
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
    decode_usize(word_at(data, word_index)?)
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

fn normalize_word_hex(word: &[u8]) -> String {
    format!("0x{}", hex_string(word))
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

fn null_if_zero_address(value: &str) -> Value {
    if normalize_address(value) == "0x0000000000000000000000000000000000000000" {
        Value::Null
    } else {
        Value::String(normalize_address(value))
    }
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

    use anyhow::{Context, Result};
    use bigname_storage::{default_database_url, upsert_normalized_events};
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
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
                .context("failed to parse database URL for ENSv2 registrar tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_ens_v2_registrar_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for ENSv2 registrar tests")?;
            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for ENSv2 registrar tests")?;
            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for ENSv2 registrar tests")?;

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
    async fn ens_v2_registrar_links_pre_regeneration_token_to_registry_resource() -> Result<()> {
        let database = TestDatabase::new().await?;
        let old_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a1";
        let new_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a2";
        let resource_id = Uuid::from_u128(0xfeed);
        let logical_name_id = "ens:alice.eth";

        upsert_normalized_events(
            database.pool(),
            &[
                registry_event(
                    "token-resource",
                    logical_name_id,
                    resource_id,
                    "TokenResourceLinked",
                    10,
                    json!({
                        "token_id": old_token_id,
                        "current_token_id": new_token_id,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                ),
                registry_event(
                    "token-regenerated",
                    logical_name_id,
                    resource_id,
                    "TokenRegenerated",
                    11,
                    json!({
                        "old_token_id": old_token_id,
                        "new_token_id": new_token_id,
                    }),
                ),
            ],
        )
        .await?;

        let event = build_registrar_event(
            database.pool(),
            &raw_log(),
            RegistrarObservation::NameRenewed {
                token_id: old_token_id.to_owned(),
                label: "alice".to_owned(),
                duration: 31_536_000,
                new_expiry: 2_000_000_000,
                payment_token: ZERO_ADDRESS_FOR_TEST.to_owned(),
                referrer: format!("0x{}", "00".repeat(32)),
                base: "0x01".to_owned(),
            },
        )
        .await?;

        assert_eq!(event.logical_name_id, Some(logical_name_id.to_owned()));
        assert_eq!(event.resource_id, Some(resource_id));
        assert_eq!(
            event.after_state["token_id"],
            Value::String(old_token_id.to_owned())
        );
        assert_eq!(
            event.after_state["registry_resource_id"],
            Value::String(resource_id.to_string())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn ens_v2_registrar_links_post_regeneration_token_to_registry_resource() -> Result<()> {
        let database = TestDatabase::new().await?;
        let old_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a1";
        let new_token_id = "0x00000000000000000000000000000000000000000000000000000000000000a2";
        let resource_id = Uuid::from_u128(0xfeee);
        let logical_name_id = "ens:alice.eth";

        upsert_normalized_events(
            database.pool(),
            &[
                registry_event(
                    "token-resource-new-path",
                    logical_name_id,
                    resource_id,
                    "TokenResourceLinked",
                    10,
                    json!({
                        "token_id": old_token_id,
                        "current_token_id": new_token_id,
                        "upstream_resource": "0x0000000000000000000000000000000000000000000000000000000000000eac",
                    }),
                ),
                registry_event(
                    "token-regenerated-new-path",
                    logical_name_id,
                    resource_id,
                    "TokenRegenerated",
                    11,
                    json!({
                        "old_token_id": old_token_id,
                        "new_token_id": new_token_id,
                    }),
                ),
            ],
        )
        .await?;

        let event = build_registrar_event(
            database.pool(),
            &raw_log(),
            RegistrarObservation::NameRenewed {
                token_id: new_token_id.to_owned(),
                label: "alice".to_owned(),
                duration: 31_536_000,
                new_expiry: 2_000_000_000,
                payment_token: ZERO_ADDRESS_FOR_TEST.to_owned(),
                referrer: format!("0x{}", "00".repeat(32)),
                base: "0x01".to_owned(),
            },
        )
        .await?;

        assert_eq!(event.logical_name_id, Some(logical_name_id.to_owned()));
        assert_eq!(event.resource_id, Some(resource_id));
        assert_eq!(
            event.after_state["token_id"],
            Value::String(new_token_id.to_owned())
        );
        assert_eq!(
            event.after_state["registry_resource_id"],
            Value::String(resource_id.to_string())
        );

        database.cleanup().await
    }

    fn registry_event(
        suffix: &str,
        logical_name_id: &str,
        resource_id: Uuid,
        event_kind: &str,
        block_number: i64,
        after_state: Value,
    ) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("ens-v2-registrar-test:{suffix}"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xblock{block_number}")),
            transaction_hash: Some(format!("0xtx{block_number}")),
            log_index: Some(0),
            raw_fact_ref: json!({"source": "ens_v2_registrar_test"}),
            derivation_kind: REGISTRY_DERIVATION_KIND.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state,
        }
    }

    const ZERO_ADDRESS_FOR_TEST: &str = "0x0000000000000000000000000000000000000000";

    fn raw_log() -> RegistrarRawLogRow {
        RegistrarRawLogRow {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xregistrar".to_owned(),
            block_number: 12,
            transaction_hash: "0xtxregistrar".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000ee".to_owned(),
            topics: Vec::new(),
            data: Vec::new(),
            canonicality_state: CanonicalityState::Finalized,
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned(),
            manifest_version: 1,
        }
    }
}
