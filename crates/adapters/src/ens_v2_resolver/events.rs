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
            let from_decoded = match dns_decode_optional(&from_name) {
                Ok(value) => value,
                Err(_) => return Ok(Vec::new()),
            };
            let to_decoded = dns_decode_optional(&to_name).ok().flatten();
            let alias_removed = to_name.is_empty();
            let alias_unknown = !alias_removed && to_decoded.is_none();
            let from_logical_name_id = from_decoded
                .as_ref()
                .filter(|name| !name.is_empty())
                .map(|name| logical_name_id(&raw_log.namespace, name));
            let to_link = if alias_removed || alias_unknown {
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
                    "alias_state": if alias_removed { "removed" } else if alias_unknown { "unknown" } else { "active" },
                    "active": !alias_removed && !alias_unknown,
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
        if let Ok(observation) = observe_dns_encoded_name(from_name) {
            events.push(preimage_observed_event(
                raw_log,
                "AliasChanged",
                observation,
                Some("from_name"),
            ));
        }
    }
    if !to_name.is_empty() {
        if let Ok(observation) = observe_dns_encoded_name(to_name) {
            events.push(preimage_observed_event(
                raw_log,
                "AliasChanged",
                observation,
                Some("to_name"),
            ));
        }
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
    let Ok(observation) = observe_dns_encoded_name(name) else {
        return Ok(Vec::new());
    };
    Ok(vec![preimage_observed_event(
        raw_log,
        source_event,
        observation,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::CanonicalityState;
    use sqlx::{postgres::PgPoolOptions, types::time::OffsetDateTime};

    fn raw_log() -> ResolverRawLogRow {
        ResolverRawLogRow {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 1,
            event_position_timestamp: OffsetDateTime::UNIX_EPOCH,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            emitting_contract_instance_id: Uuid::nil(),
            topics: Vec::new(),
            data: Vec::new(),
            canonicality_state: CanonicalityState::Canonical,
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_resolver_l1".to_owned(),
            manifest_version: 1,
        }
    }

    fn dns_name(labels: &[&str]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for label in labels {
            encoded.push(u8::try_from(label.len()).expect("label length fits"));
            encoded.extend_from_slice(label.as_bytes());
        }
        encoded.push(0);
        encoded
    }

    #[tokio::test]
    async fn alias_changed_skips_invalid_source_name_without_pool_query() -> Result<()> {
        let pool =
            PgPoolOptions::new().connect_lazy("postgres://bigname:bigname@127.0.0.1:1/bigname")?;
        let events = build_resolver_events(
            &pool,
            &raw_log(),
            ResolverObservation::AliasChanged {
                from_name: dns_name(&["Ni\u{200d}ck", "eth"]),
                to_name: dns_name(&["alice", "eth"]),
            },
        )
        .await?;

        assert!(events.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn alias_changed_treats_invalid_target_name_as_unknown_without_pool_query() -> Result<()>
    {
        let pool =
            PgPoolOptions::new().connect_lazy("postgres://bigname:bigname@127.0.0.1:1/bigname")?;
        let events = build_resolver_events(
            &pool,
            &raw_log(),
            ResolverObservation::AliasChanged {
                from_name: dns_name(&["alice", "eth"]),
                to_name: dns_name(&["Ni\u{200d}ck", "eth"]),
            },
        )
        .await?;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].after_state["alias_state"], "unknown");
        assert_eq!(events[0].after_state["active"], false);
        assert_eq!(events[0].after_state["to_name"], Value::Null);
        assert_eq!(events[0].resource_id, None);
        assert_eq!(events[1].event_kind, EVENT_KIND_PREIMAGE_OBSERVED);
        assert_eq!(events[1].after_state["observation_slot"], "from_name");
        Ok(())
    }
}
