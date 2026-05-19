use anyhow::Result;
use bigname_storage::{CanonicalityState, NormalizedEvent};
use sqlx::types::{Uuid, time::OffsetDateTime};
use std::collections::HashMap;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::keccak_signature_hex;

use super::{
    constants::*,
    decode::build_resolver_observation,
    events::{alias_preimage_events, named_dns_preimage_events},
    types::{ResolverObservation, ResolverRawLogRow},
};

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

    let event_topics = test_resolver_event_topics();
    let Some(observation) = build_resolver_observation(&raw_log, &event_topics)? else {
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

fn test_resolver_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(HashMap::from([
        (
            ABI_EVENT_ADDRESS_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_ADDRESS_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_TEXT_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_TEXT_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_NAME_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAME_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_VERSION_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_VERSION_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_ALIAS_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_ALIAS_CHANGED_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE),
        ),
    ]))
}
