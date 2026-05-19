use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::{constants::*, types::ObservationRef};

pub(super) fn normalized_event(
    reference: &ObservationRef,
    logical_name_id: Option<String>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    before_state: Value,
    after_state: Value,
    identity_suffix: String,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "ens_v2_registry_resource_surface:{}:{}:{}:{}:{}:{}",
            reference.source_manifest_id,
            reference.block_hash,
            reference.transaction_hash,
            reference.log_index,
            event_kind,
            identity_suffix
        ),
        namespace: reference.namespace.clone(),
        logical_name_id,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: reference.source_family.clone(),
        manifest_version: reference.manifest_version,
        source_manifest_id: Some(reference.source_manifest_id),
        chain_id: Some(reference.chain_id.clone()),
        block_number: Some(reference.block_number),
        block_hash: Some(reference.block_hash.clone()),
        transaction_hash: Some(reference.transaction_hash.clone()),
        log_index: Some(reference.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": reference.chain_id,
            "block_hash": reference.block_hash,
            "block_number": reference.block_number,
            "transaction_hash": reference.transaction_hash,
            "transaction_index": reference.transaction_index,
            "log_index": reference.log_index,
            "emitting_address": reference.emitting_address,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE.to_owned(),
        canonicality_state: reference.canonicality_state,
        before_state,
        after_state,
    }
}
