use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::load_watched_contracts;
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time::OffsetDateTime},
};

const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(crate) const DERIVATION_KIND_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str = "raw_log_preimage_observation";
const RESOLVER_EDGE_KIND: &str = "resolver";

const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";

const ADDRESS_CHANGED_SIGNATURE: &str = "AddressChanged(bytes32,uint256,bytes)";
const TEXT_CHANGED_SIGNATURE: &str = "TextChanged(bytes32,string,string,string)";
const CONTENTHASH_CHANGED_SIGNATURE: &str = "ContenthashChanged(bytes32,bytes)";
const NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
const VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";
const ALIAS_CHANGED_SIGNATURE: &str = "AliasChanged(bytes,bytes,bytes,bytes)";
const NAMED_RESOURCE_SIGNATURE: &str = "NamedResource(uint256,bytes)";
const NAMED_TEXT_RESOURCE_SIGNATURE: &str = "NamedTextResource(uint256,bytes,bytes32,string)";
const NAMED_ADDR_RESOURCE_SIGNATURE: &str = "NamedAddrResource(uint256,bytes,uint256)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2ResolverSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2ResolverKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2ResolverKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV2ResolverSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_resolver_with_scope(pool, chain, true, block_hashes).await
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverRawLogRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    event_position_timestamp: OffsetDateTime,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: Uuid,
    topics: Vec<String>,
    data: Vec<u8>,
    canonicality_state: CanonicalityState,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NameLink {
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    normalized_name: Option<String>,
    canonical_display_name: Option<String>,
    namehash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreimageObservation {
    dns_encoded_name: String,
    decoded_name: Option<String>,
    labelhashes: Vec<String>,
    namehash: String,
}

enum ResolverObservation {
    AddressChanged {
        node: String,
        coin_type: String,
        address_bytes: Vec<u8>,
    },
    TextChanged {
        node: String,
        key: String,
        value: String,
    },
    ContenthashChanged {
        node: String,
        hash: Vec<u8>,
    },
    NameChanged {
        node: String,
        name: String,
    },
    VersionChanged {
        node: String,
        version: i64,
    },
    AliasChanged {
        from_name: Vec<u8>,
        to_name: Vec<u8>,
    },
    NamedResource {
        name: Vec<u8>,
    },
    NamedTextResource {
        name: Vec<u8>,
    },
    NamedAddrResource {
        name: Vec<u8>,
    },
}

pub async fn sync_ens_v2_resolver(pool: &PgPool, chain: &str) -> Result<EnsV2ResolverSyncSummary> {
    sync_ens_v2_resolver_with_scope(pool, chain, false, &[]).await
}

async fn sync_ens_v2_resolver_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV2ResolverSyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(empty_summary(0));
    }

    let raw_logs = load_resolver_raw_logs(
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
        let Some(observation) = build_resolver_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;
        events.extend(build_resolver_events(pool, raw_log, observation).await?);
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
                EnsV2ResolverKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(EnsV2ResolverSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    })
}

async fn build_resolver_events(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    observation: ResolverObservation,
) -> Result<Vec<NormalizedEvent>> {
    match observation {
        ResolverObservation::AddressChanged {
            node,
            coin_type,
            address_bytes,
        } => {
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            Ok(vec![normalized_event(
                raw_log,
                link.logical_name_id,
                link.resource_id,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                json!({
                    "source_event": "AddressChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "node": node,
                    "record_key": format!("addr:{coin_type}"),
                    "record_family": "addr",
                    "selector_key": coin_type,
                    "value_retained": false,
                    "address_bytes_hex": format!("0x{}", hex_string(address_bytes)),
                }),
                "address-changed",
            )])
        }
        ResolverObservation::TextChanged { node, key, value } => {
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            Ok(vec![normalized_event(
                raw_log,
                link.logical_name_id,
                link.resource_id,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                json!({
                    "source_event": "TextChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "node": node,
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": Value::Null,
                    "text_key": key,
                    "value_retained": false,
                    "value_length": value.len(),
                }),
                "text-changed",
            )])
        }
        ResolverObservation::ContenthashChanged { node, hash } => {
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            Ok(vec![normalized_event(
                raw_log,
                link.logical_name_id,
                link.resource_id,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                json!({
                    "source_event": "ContenthashChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "node": node,
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": Value::Null,
                    "value_retained": false,
                    "contenthash_hex": format!("0x{}", hex_string(hash)),
                }),
                "contenthash-changed",
            )])
        }
        ResolverObservation::NameChanged { node, name } => {
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            Ok(vec![normalized_event(
                raw_log,
                link.logical_name_id,
                link.resource_id,
                EVENT_KIND_RECORD_CHANGED,
                json!({}),
                json!({
                    "source_event": "NameChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "node": node,
                    "record_key": "name",
                    "record_family": "name",
                    "selector_key": Value::Null,
                    "value_retained": false,
                    "value_length": name.len(),
                }),
                "name-changed",
            )])
        }
        ResolverObservation::VersionChanged { node, version } => {
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            Ok(vec![normalized_event(
                raw_log,
                link.logical_name_id,
                link.resource_id,
                EVENT_KIND_RECORD_VERSION_CHANGED,
                json!({}),
                json!({
                    "source_event": "VersionChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "node": node,
                    "record_version": version.to_string(),
                }),
                "version-changed",
            )])
        }
        ResolverObservation::AliasChanged { from_name, to_name } => {
            let from_decoded = dns_decode_optional(&from_name)?;
            let to_decoded = dns_decode_optional(&to_name)?;
            let alias_removed = matches!(to_decoded.as_deref(), None | Some(""));
            let from_logical_name_id = from_decoded
                .as_ref()
                .filter(|name| !name.is_empty())
                .map(|name| logical_name_id(&raw_log.namespace, name));
            let to_link = if alias_removed {
                NameLink::unknown()
            } else {
                load_name_link_by_name(
                    pool,
                    raw_log,
                    to_decoded
                        .as_deref()
                        .context("active alias is missing target name")?,
                )
                .await?
            };
            let mut events = vec![normalized_event(
                raw_log,
                from_logical_name_id,
                to_link.resource_id,
                EVENT_KIND_ALIAS_CHANGED,
                json!({}),
                json!({
                    "source_event": "AliasChanged",
                    "resolver": raw_log.emitting_address,
                    "resolver_contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
                    "from_dns_encoded_name": format!("0x{}", hex_string(&from_name)),
                    "to_dns_encoded_name": format!("0x{}", hex_string(&to_name)),
                    "alias_state": if alias_removed { "removed" } else { "active" },
                    "active": !alias_removed,
                    "from_name": from_decoded,
                    "to_name": to_decoded,
                    "to_logical_name_id": to_link.logical_name_id,
                    "to_resource_id": to_link.resource_id.map(|value| value.to_string()),
                    "to_normalized_name": to_link.normalized_name,
                    "to_canonical_display_name": to_link.canonical_display_name,
                    "to_namehash": to_link.namehash,
                }),
                "alias-changed",
            )];
            events.extend(alias_preimage_events(raw_log, &from_name, &to_name)?);
            Ok(events)
        }
        ResolverObservation::NamedResource { name } => {
            named_dns_preimage_events(raw_log, "NamedResource", &name)
        }
        ResolverObservation::NamedTextResource { name } => {
            named_dns_preimage_events(raw_log, "NamedTextResource", &name)
        }
        ResolverObservation::NamedAddrResource { name } => {
            named_dns_preimage_events(raw_log, "NamedAddrResource", &name)
        }
    }
}

fn build_resolver_observation(raw_log: &ResolverRawLogRow) -> Result<Option<ResolverObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ADDRESS_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("AddressChanged missing node topic")?,
        )?;
        let coin_type = decode_u256_word_decimal(&raw_log.data, 0)?;
        let address_bytes = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node,
            coin_type,
            address_bytes,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TEXT_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TextChanged missing node topic")?,
        )?;
        let key = decode_dynamic_string(&raw_log.data, 0)?;
        let value = decode_dynamic_string(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::TextChanged { node, key, value }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(CONTENTHASH_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ContenthashChanged missing node topic")?,
        )?;
        let hash = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::ContenthashChanged { node, hash }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameChanged missing node topic")?,
        )?;
        let name = decode_dynamic_string(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NameChanged { node, name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(VERSION_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("VersionChanged missing node topic")?,
        )?;
        let version = decode_u64_word(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::VersionChanged { node, version }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ALIAS_CHANGED_SIGNATURE)) {
        let from_name = decode_dynamic_bytes(&raw_log.data, 0)?;
        let to_name = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name,
            to_name,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedTextResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedAddrResource { name }));
    }

    Ok(None)
}

fn normalized_event(
    raw_log: &ResolverRawLogRow,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: Value,
    after_state: Value,
    identity_suffix: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "ens_v2_resolver:{}:{}:{}:{}:{}:{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            event_kind,
            identity_suffix
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id,
        resource_id,
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
        derivation_kind: DERIVATION_KIND_ENS_V2_RESOLVER.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state,
        after_state,
    }
}

fn alias_preimage_events(
    raw_log: &ResolverRawLogRow,
    from_name: &[u8],
    to_name: &[u8],
) -> Result<Vec<NormalizedEvent>> {
    let mut events = Vec::new();
    if !from_name.is_empty() {
        events.push(preimage_observed_event(
            raw_log,
            "AliasChanged",
            observe_dns_encoded_name(from_name)?,
            Some("from_name"),
        ));
    }
    if !to_name.is_empty() {
        events.push(preimage_observed_event(
            raw_log,
            "AliasChanged",
            observe_dns_encoded_name(to_name)?,
            Some("to_name"),
        ));
    }
    Ok(events)
}

fn named_dns_preimage_events(
    raw_log: &ResolverRawLogRow,
    source_event: &str,
    name: &[u8],
) -> Result<Vec<NormalizedEvent>> {
    if name.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![preimage_observed_event(
        raw_log,
        source_event,
        observe_dns_encoded_name(name)?,
        None,
    )])
}

fn preimage_observed_event(
    raw_log: &ResolverRawLogRow,
    source_event: &str,
    observation: PreimageObservation,
    observation_slot: Option<&str>,
) -> NormalizedEvent {
    let identity_suffix = observation_slot
        .map(|slot| format!(":{slot}"))
        .unwrap_or_default();
    let mut after_state = json!({
        "source_event": source_event,
        "dns_encoded_name": observation.dns_encoded_name,
        "decoded_name": observation.decoded_name,
        "labelhashes": observation.labelhashes,
        "namehash": observation.namehash,
    });
    if let Some(observation_slot) = observation_slot
        && let Some(object) = after_state.as_object_mut()
    {
        object.insert(
            "observation_slot".to_owned(),
            Value::String(observation_slot.to_owned()),
        );
    }
    NormalizedEvent {
        event_identity: format!(
            "raw_log_preimage_observed:{}:{}:{}:{}:{}{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            raw_log.emitting_address,
            identity_suffix
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
        raw_fact_ref: raw_log_preimage_fact_ref(raw_log),
        derivation_kind: DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    }
}

fn raw_log_preimage_fact_ref(raw_log: &ResolverRawLogRow) -> Value {
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
        "topic1": raw_log.topics.get(1).cloned(),
        "topic2": raw_log.topics.get(2).cloned(),
        "data_hex": hex_string(&raw_log.data),
    })
}

#[cfg(test)]
pub(crate) mod testsupport {
    use super::*;

    #[derive(Clone, Debug)]
    pub(crate) struct ResolverPreimageRawLog {
        pub(crate) chain_id: String,
        pub(crate) block_hash: String,
        pub(crate) block_number: i64,
        pub(crate) transaction_hash: String,
        pub(crate) transaction_index: i64,
        pub(crate) log_index: i64,
        pub(crate) emitting_address: String,
        pub(crate) topics: Vec<String>,
        pub(crate) data: Vec<u8>,
        pub(crate) canonicality_state: CanonicalityState,
        pub(crate) source_manifest_id: i64,
        pub(crate) namespace: String,
        pub(crate) source_family: String,
        pub(crate) manifest_version: i64,
    }

    pub(crate) fn build_preimage_observed_events(
        input: ResolverPreimageRawLog,
    ) -> Result<Vec<NormalizedEvent>> {
        let raw_log = ResolverRawLogRow {
            chain_id: input.chain_id,
            block_hash: input.block_hash,
            block_number: input.block_number,
            event_position_timestamp: OffsetDateTime::UNIX_EPOCH,
            transaction_hash: input.transaction_hash,
            transaction_index: input.transaction_index,
            log_index: input.log_index,
            emitting_address: input.emitting_address,
            emitting_contract_instance_id: Uuid::nil(),
            topics: input.topics,
            data: input.data,
            canonicality_state: input.canonicality_state,
            source_manifest_id: input.source_manifest_id,
            namespace: input.namespace,
            source_family: input.source_family,
            manifest_version: input.manifest_version,
        };

        let Some(observation) = build_resolver_observation(&raw_log)? else {
            return Ok(Vec::new());
        };
        match observation {
            ResolverObservation::AliasChanged { from_name, to_name } => {
                alias_preimage_events(&raw_log, &from_name, &to_name)
            }
            ResolverObservation::NamedResource { name } => {
                named_dns_preimage_events(&raw_log, "NamedResource", &name)
            }
            ResolverObservation::NamedTextResource { name } => {
                named_dns_preimage_events(&raw_log, "NamedTextResource", &name)
            }
            ResolverObservation::NamedAddrResource { name } => {
                named_dns_preimage_events(&raw_log, "NamedAddrResource", &name)
            }
            _ => Ok(Vec::new()),
        }
    }
}

async fn load_name_link_by_namehash(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    namehash: &str,
) -> Result<NameLink> {
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND lower(ns.namehash) = lower($2)
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(namehash)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for namespace {} node {namehash} at chain position",
            raw_log.namespace
        )
    })?;

    row.map(decode_name_link)
        .transpose()
        .map(|link| link.unwrap_or_else(NameLink::unknown))
}

async fn load_name_link_by_name(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    name: &str,
) -> Result<NameLink> {
    let normalized_name = name.to_ascii_lowercase();
    if normalized_name.is_empty() {
        return Ok(NameLink::unknown());
    }
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND ns.normalized_name = $2
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(&normalized_name)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for {}:{normalized_name} at chain position",
            raw_log.namespace
        )
    })?;

    Ok(row.map(decode_name_link).transpose()?.unwrap_or(NameLink {
        logical_name_id: Some(logical_name_id(&raw_log.namespace, &normalized_name)),
        normalized_name: Some(normalized_name.clone()),
        canonical_display_name: Some(display_name(&normalized_name)),
        namehash: None,
        resource_id: None,
    }))
}

impl NameLink {
    fn unknown() -> Self {
        Self {
            logical_name_id: None,
            resource_id: None,
            normalized_name: None,
            canonical_display_name: None,
            namehash: None,
        }
    }
}

fn decode_name_link(row: sqlx::postgres::PgRow) -> Result<NameLink> {
    Ok(NameLink {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
    })
}

async fn load_resolver_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<Vec<ResolverRawLogRow>> {
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
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rb.block_timestamp
              + (((rl.transaction_index * 1000) + GREATEST(rl.log_index, 0)) * INTERVAL '1 microsecond')
              AS event_position_timestamp,
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
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, lower(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 resolver raw logs for chain {chain}"))?;

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
            .and_then(|emitters| emitter_for_block(emitters, block_number))
        else {
            continue;
        };
        output.push(ResolverRawLogRow {
            chain_id: row.try_get("chain_id").context("missing chain_id")?,
            block_hash: row.try_get("block_hash").context("missing block_hash")?,
            block_number,
            event_position_timestamp: row
                .try_get("event_position_timestamp")
                .context("missing event_position_timestamp")?,
            transaction_hash: row
                .try_get("transaction_hash")
                .context("missing transaction_hash")?,
            transaction_index: row
                .try_get("transaction_index")
                .context("missing transaction_index")?,
            log_index: row.try_get("log_index").context("missing log_index")?,
            emitting_address,
            emitting_contract_instance_id: emitter.contract_instance_id,
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
        });
    }
    Ok(output)
}

async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv2 resolver adapter")?;
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
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_RESOLVER_L1 {
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

        emitters_by_address.insert(
            watched_contract.address.clone(),
            ActiveEmitter {
                address: watched_contract.address,
                contract_instance_id: watched_contract.contract_instance_id,
                source_manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: None,
                active_to_block_number: None,
            },
        );
    }
    if let Some(manifest) = load_active_resolver_manifest_metadata(pool, chain).await? {
        for emitter in load_discovered_resolver_emitters(pool, chain, &manifest).await? {
            emitters_by_address
                .entry(emitter.address.clone())
                .or_insert(emitter);
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

async fn load_discovered_resolver_emitters(
    pool: &PgPool,
    chain: &str,
    manifest: &ActiveManifestMetadata,
) -> Result<Vec<ActiveEmitter>> {
    let rows = sqlx::query(
        r#"
        SELECT
            cia.address,
            de.to_contract_instance_id,
            de.active_from_block_number,
            de.active_to_block_number
        FROM discovery_edges de
        JOIN manifest_versions source_mv
          ON source_mv.manifest_id = de.source_manifest_id
         AND source_mv.rollout_status = 'active'
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
        WHERE de.chain_id = $1
          AND de.edge_kind = $2
        ORDER BY lower(cia.address), de.active_from_block_number NULLS FIRST, de.discovery_edge_id
        "#,
    )
    .bind(chain)
    .bind(RESOLVER_EDGE_KIND)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 discovered resolver emitters for {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = normalize_address(
                &row.try_get::<String, _>("address")
                    .context("missing discovered resolver address")?,
            );
            Ok(ActiveEmitter {
                address,
                contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("missing discovered resolver contract_instance_id")?,
                source_manifest_id: manifest.manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("missing active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("missing active_to_block_number")?,
            })
        })
        .collect()
}

fn emitter_for_block(emitters: &[ActiveEmitter], block_number: i64) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

async fn load_active_resolver_manifest_metadata(
    pool: &PgPool,
    chain: &str,
) -> Result<Option<ActiveManifestMetadata>> {
    let row = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND chain = $1
          AND source_family = $2
        ORDER BY manifest_version DESC, manifest_id DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load active ENSv2 resolver manifest for {chain}"))?;

    row.map(decode_active_manifest_metadata).transpose()
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
    .context("failed to load active manifest metadata for ENSv2 resolver emitters")?;

    rows.into_iter()
        .map(|row| {
            let manifest = decode_active_manifest_metadata(row)?;
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

fn decode_active_manifest_metadata(row: sqlx::postgres::PgRow) -> Result<ActiveManifestMetadata> {
    Ok(ActiveManifestMetadata {
        manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
        chain: row.try_get("chain").context("missing chain")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
    })
}

async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }

    let identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(&identities)
    .fetch_all(pool)
    .await
    .context("failed to load existing ENSv2 resolver event identities")?;

    rows.into_iter()
        .map(|row| {
            row.try_get("event_identity")
                .context("missing event_identity")
        })
        .collect()
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
    existing: &HashSet<String>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        if !existing.contains(&event.event_identity) {
            *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
        }
    }
    counts
}

fn empty_summary(scanned_log_count: usize) -> EnsV2ResolverSyncSummary {
    EnsV2ResolverSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn raw_fact_ref(raw_log: &ResolverRawLogRow) -> Value {
    json!({
        "kind": "raw_log",
        "chain_id": raw_log.chain_id,
        "block_hash": raw_log.block_hash,
        "block_number": raw_log.block_number,
        "transaction_hash": raw_log.transaction_hash,
        "transaction_index": raw_log.transaction_index,
        "log_index": raw_log.log_index,
        "emitting_address": raw_log.emitting_address,
    })
}

fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(decode_dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string is not valid UTF-8")
}

fn decode_dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    let offset = decode_usize_word(data, offset_word_index)?;
    if data.len() < offset + 32 {
        bail!("dynamic bytes payload is missing length word");
    }
    let length = decode_usize_at(data, offset)?;
    let start = offset + 32;
    let end = start + length;
    if data.len() < end {
        bail!("dynamic bytes payload is shorter than declared length");
    }
    Ok(data[start..end].to_vec())
}

fn decode_u256_word_decimal(data: &[u8], word_index: usize) -> Result<String> {
    let word = word_at(data, word_index)?;
    Ok(decimal_string_from_be_bytes(word))
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

fn event_position_timestamp(raw_log: &ResolverRawLogRow) -> OffsetDateTime {
    raw_log.event_position_timestamp
}

fn dns_decode_optional(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        dns_decode(bytes).map(Some)
    }
}

fn dns_decode(bytes: &[u8]) -> Result<String> {
    let mut labels = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let length = bytes[index] as usize;
        index += 1;
        if length == 0 {
            if index != bytes.len() {
                bail!("DNS-encoded name has trailing bytes");
            }
            return Ok(labels.join(".").to_ascii_lowercase());
        }
        let end = index + length;
        if end > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(
            String::from_utf8(bytes[index..end].to_vec())
                .context("DNS-encoded label is not valid UTF-8")?,
        );
        index = end;
    }
    bail!("DNS-encoded name is missing root label")
}

fn observe_dns_encoded_name(bytes: &[u8]) -> Result<PreimageObservation> {
    if bytes.is_empty() {
        bail!("DNS-encoded name payload must not be empty");
    }

    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            bail!("DNS-encoded name payload is missing root label");
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                bail!("DNS-encoded name payload has trailing bytes");
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(bytes[cursor..cursor + label_length].to_vec());
        cursor += label_length;
    }

    let decoded_name = labels
        .iter()
        .map(|label| String::from_utf8(label.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()
        .map(|labels| labels.join("."));
    let labelhashes = labels
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();

    Ok(PreimageObservation {
        dns_encoded_name: format!("0x{}", hex_string(bytes)),
        decoded_name,
        labelhashes,
        namehash: namehash_hex(&labels),
    })
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
    format!("0x{}", hex_string(node))
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

fn logical_name_id(namespace: &str, name: &str) -> String {
    if name.is_empty() {
        format!("{namespace}:")
    } else {
        format!("{namespace}:{}", name.to_ascii_lowercase())
    }
}

fn decimal_string_from_be_bytes(bytes: &[u8]) -> String {
    let mut digits = vec![0u8];
    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    digits
        .into_iter()
        .skip_while(|digit| *digit == 0)
        .map(|digit| char::from(b'0' + digit))
        .collect::<String>()
        .if_empty_then_zero()
}

trait EmptyThenZero {
    fn if_empty_then_zero(self) -> Self;
}

impl EmptyThenZero for String {
    fn if_empty_then_zero(self) -> Self {
        if self.is_empty() {
            "0".to_owned()
        } else {
            self
        }
    }
}

fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex_string(keccak256_bytes(bytes)))
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
