use anyhow::{Context, Result};
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};
use sqlx::{PgPool, types::Uuid};

use super::{
    DERIVATION_KIND_ENS_V2_RESOLVER,
    constants::{
        DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION, EVENT_KIND_ALIAS_CHANGED,
        EVENT_KIND_PREIMAGE_OBSERVED, EVENT_KIND_RECORD_CHANGED, EVENT_KIND_RECORD_VERSION_CHANGED,
    },
    queries::{load_name_link_by_name, load_name_link_by_namehash},
    types::{NameLink, PreimageObservation, ResolverObservation, ResolverRawLogRow},
    util::{dns_decode_optional, hex_string, logical_name_id, observe_dns_encoded_name},
};

pub(super) async fn build_resolver_events(
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
            if key.trim().is_empty() {
                return Ok(Vec::new());
            }
            let link = load_name_link_by_namehash(pool, raw_log, &node).await?;
            let value_length = value.len();
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
                    "record_key": format!("text:{key}"),
                    "record_family": "text",
                    "selector_key": key.clone(),
                    "text_key": key,
                    "value_retained": true,
                    "value": value,
                    "value_length": value_length,
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

pub(super) fn alias_preimage_events(
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

pub(super) fn named_dns_preimage_events(
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
