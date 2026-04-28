use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};

use super::constants::{
    DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION, ENS_V2_ALIAS_CHANGED_SIGNATURE,
    ENS_V2_LABEL_REGISTERED_SIGNATURE, ENS_V2_LABEL_RESERVED_SIGNATURE,
    ENS_V2_NAMED_ADDR_RESOURCE_SIGNATURE, ENS_V2_NAMED_RESOURCE_SIGNATURE,
    ENS_V2_NAMED_TEXT_RESOURCE_SIGNATURE, ENS_V2_PARENT_UPDATED_SIGNATURE,
    ENS_V2_REGISTRAR_NAME_REGISTERED_SIGNATURE, ENS_V2_REGISTRAR_NAME_RENEWED_SIGNATURE,
    EVENT_KIND_PREIMAGE_OBSERVED, SOURCE_EVENT_ALIAS_CHANGED, SOURCE_EVENT_LABEL_REGISTERED,
    SOURCE_EVENT_LABEL_RESERVED, SOURCE_EVENT_NAME_REGISTERED, SOURCE_EVENT_NAME_RENEWED,
    SOURCE_EVENT_NAME_WRAPPED, SOURCE_EVENT_NAMED_ADDR_RESOURCE, SOURCE_EVENT_NAMED_RESOURCE,
    SOURCE_EVENT_NAMED_TEXT_RESOURCE, SOURCE_EVENT_PARENT_UPDATED,
    SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
    SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::decoding::{
    decode_dynamic_bytes, decode_dynamic_string, decode_first_dynamic_string,
    hex_string_without_prefix, keccak_signature_hex, keccak256_hex, name_wrapped_topic0,
    registrar_name_registered_topic0, registrar_name_renewed_topic0,
};
use super::preimage_observation::{
    observe_dns_encoded_name, observe_registrar_eth_name, observe_single_label,
};
use super::types::{PreimageObservation, WatchedRawLogRow};

pub(super) fn preimage_observed_topic0s() -> Vec<String> {
    [
        name_wrapped_topic0(),
        registrar_name_registered_topic0(),
        registrar_name_renewed_topic0(),
        keccak_signature_hex(ENS_V2_LABEL_REGISTERED_SIGNATURE),
        keccak_signature_hex(ENS_V2_LABEL_RESERVED_SIGNATURE),
        keccak_signature_hex(ENS_V2_PARENT_UPDATED_SIGNATURE),
        keccak_signature_hex(ENS_V2_REGISTRAR_NAME_REGISTERED_SIGNATURE),
        keccak_signature_hex(ENS_V2_REGISTRAR_NAME_RENEWED_SIGNATURE),
        keccak_signature_hex(ENS_V2_ALIAS_CHANGED_SIGNATURE),
        keccak_signature_hex(ENS_V2_NAMED_RESOURCE_SIGNATURE),
        keccak_signature_hex(ENS_V2_NAMED_TEXT_RESOURCE_SIGNATURE),
        keccak_signature_hex(ENS_V2_NAMED_ADDR_RESOURCE_SIGNATURE),
    ]
    .into_iter()
    .collect()
}

pub(super) fn build_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let events = build_registrar_preimage_observed_events(raw_log)?;
    if !events.is_empty() {
        return Ok(events);
    }

    let events = build_ens_v2_preimage_observed_events(raw_log)?;
    if !events.is_empty() {
        return Ok(events);
    }

    build_name_wrapped_preimage_observed_events(raw_log)
}

fn build_name_wrapped_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    if !topic0.eq_ignore_ascii_case(&name_wrapped_topic0()) {
        return Ok(Vec::new());
    }

    let dns_name = decode_dynamic_bytes(&raw_log.data, 0).with_context(|| {
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

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        SOURCE_EVENT_NAME_WRAPPED,
        observation,
        None,
    )])
}

fn build_registrar_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 {
        return Ok(Vec::new());
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    let source_event = if topic0.eq_ignore_ascii_case(&registrar_name_registered_topic0()) {
        SOURCE_EVENT_NAME_REGISTERED
    } else if topic0.eq_ignore_ascii_case(&registrar_name_renewed_topic0()) {
        SOURCE_EVENT_NAME_RENEWED
    } else {
        return Ok(Vec::new());
    };

    let label = decode_first_dynamic_string(&raw_log.data).with_context(|| {
        format!(
            "failed to decode {source_event} string label payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation = observe_registrar_eth_name(&label).with_context(|| {
        format!(
            "failed to derive registrar .eth preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observed_labelhash = observation
        .labelhashes
        .first()
        .context("registrar observation is missing the explicit labelhash")?;

    if let Some(indexed_labelhash) = raw_log.topics.get(1)
        && !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash)
    {
        bail!(
            "{source_event} indexed labelhash {} does not match decoded labelhash {} for chain {} block {} log {}",
            indexed_labelhash,
            observed_labelhash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn build_ens_v2_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };

    if is_ens_v2_registry_source(&raw_log.source_family) {
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_LABEL_REGISTERED_SIGNATURE)) {
            return build_ens_v2_registry_label_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_LABEL_REGISTERED,
            );
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_LABEL_RESERVED_SIGNATURE)) {
            return build_ens_v2_registry_label_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_LABEL_RESERVED,
            );
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_PARENT_UPDATED_SIGNATURE)) {
            let label = decode_dynamic_string(&raw_log.data, 0).with_context(|| {
                format!(
                    "failed to decode ParentUpdated string label payload for chain {} block {} log {}",
                    raw_log.chain_id, raw_log.block_hash, raw_log.log_index
                )
            })?;
            let observation = observe_single_label(&label).with_context(|| {
                format!(
                    "failed to derive ENSv2 registry parent label preimage for chain {} block {} log {}",
                    raw_log.chain_id, raw_log.block_hash, raw_log.log_index
                )
            })?;
            return Ok(vec![build_preimage_observed_normalized_event(
                raw_log,
                SOURCE_EVENT_PARENT_UPDATED,
                observation,
                None,
            )]);
        }
        return Ok(Vec::new());
    }

    if raw_log.source_family == SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 {
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(
            ENS_V2_REGISTRAR_NAME_REGISTERED_SIGNATURE,
        )) {
            return build_ens_v2_registrar_label_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_NAME_REGISTERED,
            );
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(
            ENS_V2_REGISTRAR_NAME_RENEWED_SIGNATURE,
        )) {
            return build_ens_v2_registrar_label_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_NAME_RENEWED,
            );
        }
        return Ok(Vec::new());
    }

    if raw_log.source_family == SOURCE_FAMILY_ENS_V2_RESOLVER_L1 {
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_ALIAS_CHANGED_SIGNATURE)) {
            return build_ens_v2_alias_preimage_observed_events(raw_log);
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_NAMED_RESOURCE_SIGNATURE)) {
            return build_ens_v2_named_dns_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_NAMED_RESOURCE,
                0,
                None,
            );
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_NAMED_TEXT_RESOURCE_SIGNATURE))
        {
            return build_ens_v2_named_dns_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_NAMED_TEXT_RESOURCE,
                0,
                None,
            );
        }
        if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ENS_V2_NAMED_ADDR_RESOURCE_SIGNATURE))
        {
            return build_ens_v2_named_dns_preimage_observed_events(
                raw_log,
                SOURCE_EVENT_NAMED_ADDR_RESOURCE,
                0,
                None,
            );
        }
    }

    Ok(Vec::new())
}

fn build_ens_v2_registry_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
) -> Result<Vec<NormalizedEvent>> {
    let label = decode_dynamic_string(&raw_log.data, 0).with_context(|| {
        format!(
            "failed to decode {source_event} string label payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation = observe_single_label(&label).with_context(|| {
        format!(
            "failed to derive ENSv2 registry label preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observed_labelhash = observation
        .labelhashes
        .first()
        .context("ENSv2 registry observation is missing the explicit labelhash")?;
    if let Some(indexed_labelhash) = raw_log.topics.get(2)
        && !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash)
    {
        bail!(
            "{source_event} indexed labelhash {} does not match decoded labelhash {} for chain {} block {} log {}",
            indexed_labelhash,
            observed_labelhash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn build_ens_v2_registrar_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
) -> Result<Vec<NormalizedEvent>> {
    let label = decode_dynamic_string(&raw_log.data, 0).with_context(|| {
        format!(
            "failed to decode {source_event} string label payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation = observe_registrar_eth_name(&label).with_context(|| {
        format!(
            "failed to derive ENSv2 registrar .eth preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn build_ens_v2_alias_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let from_name = decode_dynamic_bytes(&raw_log.data, 0).with_context(|| {
        format!(
            "failed to decode AliasChanged fromName payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let to_name = decode_dynamic_bytes(&raw_log.data, 1).with_context(|| {
        format!(
            "failed to decode AliasChanged toName payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    validate_indexed_bytes_hash(raw_log, 1, &from_name, "AliasChanged indexedFromName")?;
    validate_indexed_bytes_hash(raw_log, 2, &to_name, "AliasChanged indexedToName")?;

    let mut events = Vec::new();
    if !from_name.is_empty() {
        events.push(build_preimage_observed_normalized_event(
            raw_log,
            SOURCE_EVENT_ALIAS_CHANGED,
            observe_dns_encoded_name(&from_name)?,
            Some("from_name"),
        ));
    }
    if !to_name.is_empty() {
        events.push(build_preimage_observed_normalized_event(
            raw_log,
            SOURCE_EVENT_ALIAS_CHANGED,
            observe_dns_encoded_name(&to_name)?,
            Some("to_name"),
        ));
    }
    Ok(events)
}

fn build_ens_v2_named_dns_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
    offset_word_index: usize,
    observation_slot: Option<&str>,
) -> Result<Vec<NormalizedEvent>> {
    let dns_name = decode_dynamic_bytes(&raw_log.data, offset_word_index).with_context(|| {
        format!(
            "failed to decode {source_event} DNS name payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    if dns_name.is_empty() {
        return Ok(Vec::new());
    }
    let observation = observe_dns_encoded_name(&dns_name).with_context(|| {
        format!(
            "failed to interpret {source_event} DNS-encoded name for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        observation_slot,
    )])
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
    topic_index: usize,
    bytes: &[u8],
    context: &str,
) -> Result<()> {
    let Some(indexed_hash) = raw_log.topics.get(topic_index) else {
        return Ok(());
    };
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

fn is_ens_v2_registry_source(source_family: &str) -> bool {
    source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1
        || source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
}
