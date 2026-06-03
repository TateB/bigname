use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::json;

use super::helpers::{
    normalize_hex_32, normalize_topic_address, reverse_claimed_topic0_for_source_family,
    reverse_label_for_address, reverse_name_for_source_family, reverse_node_for_source_family,
    supports_reverse_claim_source_family,
};
use super::raw_logs::ReverseRawLogRow;
use super::{
    CONTRACT_ROLE_REVERSE_REGISTRAR, DERIVATION_KIND_ENS_V1_REVERSE_CLAIM, ENS_NATIVE_COIN_TYPE,
    EVENT_KIND_REVERSE_CHANGED, SOURCE_EVENT_BASE_REVERSE_CLAIMED, SOURCE_EVENT_REVERSE_CLAIMED,
    SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
};

pub(super) fn build_reverse_changed_event(
    raw_log: &ReverseRawLogRow,
) -> Result<Option<NormalizedEvent>> {
    if !supports_reverse_claim_source_family(&raw_log.source_family) {
        return Ok(None);
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let Some(expected_topic0) = reverse_claimed_topic0_for_source_family(&raw_log.source_family)
    else {
        return Ok(None);
    };
    if !topic0.eq_ignore_ascii_case(&expected_topic0) {
        return Ok(None);
    }
    let source_event = if raw_log.source_family == SOURCE_FAMILY_BASENAMES_BASE_PRIMARY {
        SOURCE_EVENT_BASE_REVERSE_CLAIMED
    } else {
        SOURCE_EVENT_REVERSE_CLAIMED
    };

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
    let reverse_name = reverse_name_for_source_family(&raw_log.source_family, &claimed_address)?;
    let derived_reverse_node =
        reverse_node_for_source_family(&raw_log.source_family, &claimed_address)?;
    if !indexed_reverse_node.eq_ignore_ascii_case(&derived_reverse_node) {
        bail!(
            "{} indexed reverse node {} does not match derived reverse node {} for chain {} block {} log {}",
            source_event,
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
            "source_event": source_event,
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
