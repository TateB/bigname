use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};

mod label_preimages;

use super::constants::{
    ALIAS_CHANGED_SIGNATURE, DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION,
    EVENT_KIND_PREIMAGE_OBSERVED, NAME_WRAPPED_SIGNATURE, NAMED_ADDR_RESOURCE_SIGNATURE,
    NAMED_RESOURCE_SIGNATURE, NAMED_TEXT_RESOURCE_SIGNATURE, SOURCE_EVENT_ALIAS_CHANGED,
    SOURCE_EVENT_NAME_WRAPPED, SOURCE_EVENT_NAMED_ADDR_RESOURCE, SOURCE_EVENT_NAMED_RESOURCE,
    SOURCE_EVENT_NAMED_TEXT_RESOURCE, SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V1_WRAPPER_L1, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::decoding::{hex_string, hex_string_without_prefix, keccak256_hex};
use super::event_topics::PreimageObservedEventTopics;
use super::preimage_observation::observe_dns_encoded_name;
use super::types::{PreimageObservation, WatchedRawLogRow};
use label_preimages::{
    build_registrar_preimage_observed_events,
    try_build_ens_v2_registrar_label_preimage_observed_events,
    try_build_ens_v2_registry_label_preimage_observed_events,
};

sol! {
    #[derive(Debug)]
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);

    #[derive(Debug)]
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);

    #[derive(Debug)]
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);

    #[derive(Debug)]
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);

    #[derive(Debug)]
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);

    #[derive(Debug)]
    event ParentUpdated(address indexed parent, string label, address indexed sender);

    #[derive(Debug)]
    event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 referrer, uint256 base, uint256 premium);

    #[derive(Debug)]
    event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 referrer, uint256 base);

    #[derive(Debug)]
    event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);

    #[derive(Debug)]
    event NamedResource(uint256 indexed resource, bytes name);

    #[derive(Debug)]
    event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);

    #[derive(Debug)]
    event NamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType);
}

pub(super) fn build_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    match raw_log.source_family.as_str() {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => {
            build_registrar_preimage_observed_events(raw_log, event_topics)
        }
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => {
            build_name_wrapped_preimage_observed_events(raw_log, event_topics)
        }
        SOURCE_FAMILY_ENS_V2_ROOT_L1
        | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        | SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
        | SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => {
            build_ens_v2_preimage_observed_events(raw_log, event_topics)
        }
        _ => Ok(Vec::new()),
    }
}

fn build_name_wrapped_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_WRAPPER_L1 {
        return Ok(Vec::new());
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    if !event_topics.matches(raw_log, NAME_WRAPPED_SIGNATURE, topic0)? {
        return Ok(Vec::new());
    }

    let event = decode_event_log::<NameWrapped>(raw_log, "NameWrapped log is malformed")
        .with_context(|| {
            format!(
                "failed to decode NameWrapped bytes payload for chain {} block {} log {}",
                raw_log.chain_id, raw_log.block_hash, raw_log.log_index
            )
        })?;
    let indexed_namehash = hex_string(event.node.as_slice());
    let dns_name = event.name.to_vec();
    let Ok(observation) = observe_dns_encoded_name(&dns_name) else {
        return Ok(Vec::new());
    };

    if !indexed_namehash.eq_ignore_ascii_case(&observation.namehash) {
        return Ok(Vec::new());
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        SOURCE_EVENT_NAME_WRAPPED,
        observation,
        None,
    )])
}

fn build_ens_v2_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };

    match raw_log.source_family.as_str() {
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
            if let Some(events) = try_build_ens_v2_registry_label_preimage_observed_events(
                raw_log,
                event_topics,
                topic0,
            )? {
                return Ok(events);
            }
            return Ok(Vec::new());
        }
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 => {
            if let Some(events) = try_build_ens_v2_registrar_label_preimage_observed_events(
                raw_log,
                event_topics,
                topic0,
            )? {
                return Ok(events);
            }
            return Ok(Vec::new());
        }
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => {
            if event_topics.matches(raw_log, ALIAS_CHANGED_SIGNATURE, topic0)? {
                return build_ens_v2_alias_preimage_observed_events(raw_log);
            }
            if event_topics.matches(raw_log, NAMED_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_RESOURCE,
                );
            }
            if event_topics.matches(raw_log, NAMED_TEXT_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_TEXT_RESOURCE,
                );
            }
            if event_topics.matches(raw_log, NAMED_ADDR_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_ADDR_RESOURCE,
                );
            }
        }
        _ => {}
    }

    Ok(Vec::new())
}

fn build_ens_v2_alias_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let event = decode_event_log::<AliasChanged>(raw_log, "AliasChanged log is malformed")
        .with_context(|| {
            format!(
                "failed to decode AliasChanged name payload for chain {} block {} log {}",
                raw_log.chain_id, raw_log.block_hash, raw_log.log_index
            )
        })?;
    let indexed_from_name = hex_string(event.indexedFromName.as_slice());
    let from_name = event.fromName.to_vec();
    let indexed_to_name = hex_string(event.indexedToName.as_slice());
    let to_name = event.toName.to_vec();
    validate_indexed_bytes_hash(
        raw_log,
        &indexed_from_name,
        &from_name,
        "AliasChanged indexedFromName",
    )?;
    validate_indexed_bytes_hash(
        raw_log,
        &indexed_to_name,
        &to_name,
        "AliasChanged indexedToName",
    )?;

    let mut events = Vec::new();
    if !from_name.is_empty() {
        if let Ok(observation) = observe_dns_encoded_name(&from_name) {
            events.push(build_preimage_observed_normalized_event(
                raw_log,
                SOURCE_EVENT_ALIAS_CHANGED,
                observation,
                Some("from_name"),
            ));
        }
    }
    if !to_name.is_empty() {
        if let Ok(observation) = observe_dns_encoded_name(&to_name) {
            events.push(build_preimage_observed_normalized_event(
                raw_log,
                SOURCE_EVENT_ALIAS_CHANGED,
                observation,
                Some("to_name"),
            ));
        }
    }
    Ok(events)
}

fn build_ens_v2_named_dns_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
) -> Result<Vec<NormalizedEvent>> {
    let dns_name = decode_named_resource_name(raw_log, source_event).with_context(|| {
        format!(
            "failed to decode {source_event} DNS name payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    if dns_name.is_empty() {
        return Ok(Vec::new());
    }
    let Ok(observation) = observe_dns_encoded_name(&dns_name) else {
        return Ok(Vec::new());
    };

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn decode_named_resource_name(raw_log: &WatchedRawLogRow, source_event: &str) -> Result<Vec<u8>> {
    match source_event {
        SOURCE_EVENT_NAMED_RESOURCE => {
            let event =
                decode_event_log::<NamedResource>(raw_log, "NamedResource log is malformed")?;
            Ok(event.name.to_vec())
        }
        SOURCE_EVENT_NAMED_TEXT_RESOURCE => {
            let event = decode_event_log::<NamedTextResource>(
                raw_log,
                "NamedTextResource log is malformed",
            )?;
            Ok(event.name.to_vec())
        }
        SOURCE_EVENT_NAMED_ADDR_RESOURCE => {
            let event = decode_event_log::<NamedAddrResource>(
                raw_log,
                "NamedAddrResource log is malformed",
            )?;
            Ok(event.name.to_vec())
        }
        _ => bail!("unsupported named resolver preimage event {source_event}"),
    }
}

fn build_preimage_observed_normalized_event(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
    observation: PreimageObservation,
    observation_slot: Option<&str>,
) -> NormalizedEvent {
    let identity_suffix = observation_slot
        .map(|slot| format!(":{}", slot))
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
            "topic2": raw_log.topics.get(2).cloned(),
            "data_hex": hex_string_without_prefix(&raw_log.data),
        }),
        derivation_kind: DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    }
}

fn validate_indexed_bytes_hash(
    raw_log: &WatchedRawLogRow,
    indexed_hash: &str,
    bytes: &[u8],
    context: &str,
) -> Result<()> {
    let observed_hash = keccak256_hex(bytes);
    if !indexed_hash.eq_ignore_ascii_case(&observed_hash) {
        bail!(
            "{context} {} does not match decoded bytes hash {} for chain {} block {} log {}",
            indexed_hash,
            observed_hash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }
    Ok(())
}

fn decode_event_log<E>(raw_log: &WatchedRawLogRow, context: &'static str) -> Result<E>
where
    E: SolEvent,
{
    crate::evm_abi::decode_event_log::<E>(&raw_log.topics, &raw_log.data, context)
}
