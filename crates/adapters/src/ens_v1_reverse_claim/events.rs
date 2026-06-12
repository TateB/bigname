use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};

use super::helpers::{
    name_for_addr_changed_topic0, normalize_hex_32, normalize_topic_address,
    reverse_claimed_topic0_for_source_family, reverse_label_for_address,
    reverse_name_for_source_family, reverse_node_for_source_family,
    supports_reverse_claim_source_family,
};
use super::raw_logs::ReverseRawLogRow;
use super::{
    BASE_NATIVE_COIN_TYPE, CONTRACT_ROLE_REVERSE_REGISTRAR, DERIVATION_KIND_ENS_V1_REVERSE_CLAIM,
    ENS_NATIVE_COIN_TYPE, EVENT_KIND_RECORD_CHANGED, EVENT_KIND_REVERSE_CHANGED,
    SOURCE_EVENT_NAME_FOR_ADDR_CHANGED, SOURCE_EVENT_REVERSE_CLAIMED,
    SOURCE_FAMILY_BASENAMES_BASE_PRIMARY, SOURCE_FAMILY_ENS_V1_REVERSE_L1,
};

pub(super) fn build_reverse_changed_events(
    raw_log: &ReverseRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    if !supports_reverse_claim_source_family(&raw_log.source_family) {
        return Ok(Vec::new());
    }

    match raw_log.source_family.as_str() {
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 => build_ens_reverse_claimed_event(raw_log),
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY => build_basenames_l2_reverse_name_events(raw_log),
        _ => Ok(Vec::new()),
    }
}

fn build_ens_reverse_claimed_event(raw_log: &ReverseRawLogRow) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    let Some(expected_topic0) = reverse_claimed_topic0_for_source_family(&raw_log.source_family)
    else {
        return Ok(Vec::new());
    };
    if !topic0.eq_ignore_ascii_case(&expected_topic0) {
        return Ok(Vec::new());
    }

    let Some(raw_claimed_address) = raw_log.topics.get(1) else {
        return Ok(Vec::new());
    };
    let Ok(claimed_address) = normalize_topic_address(raw_claimed_address) else {
        return Ok(Vec::new());
    };
    let Some(raw_indexed_reverse_node) = raw_log.topics.get(2) else {
        return Ok(Vec::new());
    };
    let Ok(indexed_reverse_node) = normalize_hex_32(raw_indexed_reverse_node) else {
        return Ok(Vec::new());
    };
    let reverse_label = reverse_label_for_address(&claimed_address)?;
    let reverse_name = reverse_name_for_source_family(&raw_log.source_family, &claimed_address)?;
    let derived_reverse_node =
        reverse_node_for_source_family(&raw_log.source_family, &claimed_address)?;
    if !indexed_reverse_node.eq_ignore_ascii_case(&derived_reverse_node) {
        return Ok(Vec::new());
    }

    Ok(vec![reverse_changed_event(
        raw_log,
        SOURCE_EVENT_REVERSE_CLAIMED,
        &claimed_address,
        ENS_NATIVE_COIN_TYPE,
        &reverse_label,
        &reverse_name,
        &derived_reverse_node,
    )])
}

fn build_basenames_l2_reverse_name_events(
    raw_log: &ReverseRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    if !topic0.eq_ignore_ascii_case(&name_for_addr_changed_topic0()) {
        return Ok(Vec::new());
    }

    let claimed_address = normalize_topic_address(
        raw_log
            .topics
            .get(1)
            .context("NameForAddrChanged log is missing indexed address")?,
    )?;
    let reverse_label = reverse_label_for_address(&claimed_address)?;
    let reverse_name = reverse_name_for_source_family(&raw_log.source_family, &claimed_address)?;
    let derived_reverse_node =
        reverse_node_for_source_family(&raw_log.source_family, &claimed_address)?;
    let raw_name = decode_abi_string(&raw_log.data)?;

    let reverse_event = reverse_changed_event(
        raw_log,
        SOURCE_EVENT_NAME_FOR_ADDR_CHANGED,
        &claimed_address,
        BASE_NATIVE_COIN_TYPE,
        &reverse_label,
        &reverse_name,
        &derived_reverse_node,
    );
    let record_event = primary_name_value_event(
        raw_log,
        &claimed_address,
        &reverse_name,
        &derived_reverse_node,
        &raw_name,
    );

    Ok(vec![reverse_event, record_event])
}

fn reverse_changed_event(
    raw_log: &ReverseRawLogRow,
    source_event: &str,
    claimed_address: &str,
    coin_type: &str,
    reverse_label: &str,
    reverse_name: &str,
    derived_reverse_node: &str,
) -> NormalizedEvent {
    NormalizedEvent {
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
            "source_event": source_event,
            "address": claimed_address,
            "coin_type": coin_type,
            "namespace": raw_log.namespace,
            "reverse_namespace": raw_log.namespace,
            "reverse_label": reverse_label,
            "reverse_name": reverse_name,
            "reverse_node": derived_reverse_node,
            "claim_provenance": claim_provenance(raw_log),
        }),
    }
}

fn primary_name_value_event(
    raw_log: &ReverseRawLogRow,
    claimed_address: &str,
    reverse_name: &str,
    derived_reverse_node: &str,
    raw_name: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_RECORD_CHANGED}:{}:{}:{}:{}:{}:name",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            claimed_address
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_RECORD_CHANGED.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: raw_fact_ref(raw_log),
        derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": SOURCE_EVENT_NAME_FOR_ADDR_CHANGED,
            "record_key": "name",
            "record_family": "name",
            "selector_key": Value::Null,
            "raw_name": raw_name,
            "primary_claim_source": {
                "address": claimed_address,
                "namespace": raw_log.namespace,
                "coin_type": BASE_NATIVE_COIN_TYPE,
                "reverse_name": reverse_name,
                "reverse_node": derived_reverse_node,
                "claim_provenance": claim_provenance(raw_log),
            },
        }),
    }
}

fn raw_fact_ref(raw_log: &ReverseRawLogRow) -> Value {
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

fn claim_provenance(raw_log: &ReverseRawLogRow) -> Value {
    json!({
        "source_family": raw_log.source_family,
        "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
        "contract_instance_id": raw_log.emitting_contract_instance_id.to_string(),
        "emitting_address": raw_log.emitting_address,
    })
}

fn decode_abi_string(data: &[u8]) -> Result<String> {
    let offset = abi_word_usize(
        data.get(..32)
            .context("NameForAddrChanged data is missing string offset")?,
    )?;
    let length_offset = offset
        .checked_add(32)
        .context("NameForAddrChanged string offset overflowed")?;
    let length = abi_word_usize(
        data.get(offset..length_offset)
            .context("NameForAddrChanged data is missing string length")?,
    )?;
    let end = length_offset
        .checked_add(length)
        .context("NameForAddrChanged string length overflowed")?;
    let bytes = data
        .get(length_offset..end)
        .context("NameForAddrChanged data is shorter than declared string length")?;
    String::from_utf8(bytes.to_vec()).context("NameForAddrChanged name is not valid utf-8")
}

fn abi_word_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word is too large for this decoder");
    }
    let mut value = [0u8; 8];
    value.copy_from_slice(&word[24..]);
    Ok(u64::from_be_bytes(value) as usize)
}
