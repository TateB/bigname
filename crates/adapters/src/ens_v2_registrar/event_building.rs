use anyhow::Result;
use bigname_domain::normalization::normalize_label_under_suffix;
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    DERIVATION_KIND_ENS_V2_REGISTRAR, EVENT_KIND_REGISTRAR_NAME_REGISTERED,
    EVENT_KIND_REGISTRATION_RENEWED,
    decoding::{RegistrarObservation, hex_string, normalize_address},
    raw_logs::RegistrarRawLogRow,
    resource_links::load_registry_resource_link,
};

pub(super) async fn build_registrar_event(
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
        normalize_label_under_suffix(&label, &["eth"])
            .ok()
            .map(|name| format!("{}:{}", raw_log.namespace, name.normalized_name))
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

fn null_if_zero_address(value: &str) -> Value {
    if normalize_address(value) == "0x0000000000000000000000000000000000000000" {
        Value::Null
    } else {
        Value::String(normalize_address(value))
    }
}
