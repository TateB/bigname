use anyhow::Result;
use bigname_storage::CanonicalityState;
use serde_json::json;
use sqlx::types::Uuid;
use std::collections::HashMap;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::keccak_signature_hex;

use super::constants::*;
use super::decode::build_permissions_observation;
use super::hints::{fallback_resource_hint, resolver_resource_hint};
use super::normalized::permission_changed_event;
use super::types::{PermissionsObservation, PermissionsRawLogRow};
use super::util::hex_string;

#[test]
fn decodes_named_text_resource_observation() -> Result<()> {
    let resource = topic_word(0x42);
    let key_hash = topic_word(0xab);
    let name = dns_name("Alice", "ETH");
    let key = b"avatar";
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("NamedTextResource(uint256,bytes,bytes32,string)"),
            resource.clone(),
            key_hash.clone(),
        ],
        encode_two_dynamic(&name, key),
    );

    let event_topics = test_permissions_event_topics();
    let observation = build_permissions_observation(&raw_log, &event_topics)?;

    match observation {
        Some(PermissionsObservation::NamedTextResource {
            resource: decoded_resource,
            name: decoded_name,
            key_hash: decoded_key_hash,
            key: decoded_key,
        }) => {
            assert_eq!(decoded_resource, resource);
            assert_eq!(decoded_name, name);
            assert_eq!(decoded_key_hash, key_hash);
            assert_eq!(decoded_key, "avatar");
        }
        _ => panic!("expected NamedTextResource observation"),
    }
    Ok(())
}

#[test]
fn builds_resource_hints_for_observed_and_fallback_resources() -> Result<()> {
    let raw_log = raw_log(Vec::new(), Vec::new());
    let resource = topic_word(0x33);
    let name = dns_name("Alice", "ETH");

    let hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;

    assert_eq!(hint.upstream_resource, resource);
    assert_eq!(hint.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(hint.normalized_name.as_deref(), Some("alice.eth"));
    assert_eq!(hint.dns_encoded_name.as_deref(), Some(name.as_slice()));
    assert_eq!(hint.selector_kind, "addr");
    assert_eq!(hint.selector_key.as_deref(), Some("60"));

    let invalid_name = dns_name("Ni\u{200d}ck", "eth");
    let invalid_hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        invalid_name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;
    assert_eq!(invalid_hint.upstream_resource, resource);
    assert_eq!(invalid_hint.logical_name_id, None);
    assert_eq!(invalid_hint.normalized_name, None);
    assert_eq!(
        invalid_hint.dns_encoded_name.as_deref(),
        Some(invalid_name.as_slice())
    );

    let root = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let fallback = fallback_resource_hint(&raw_log, root.to_owned(), true);
    assert_eq!(fallback.logical_name_id, None);
    assert_eq!(fallback.selector_kind, "root");

    Ok(())
}

#[test]
fn builds_permission_changed_event_payload() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let resource = topic_word(0x33);
    let name = dns_name("alice", "eth");
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint =
        resolver_resource_hint(&raw_log, resource.clone(), name.clone(), "name", None, None)?;
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(
        event.event_identity,
        format!("ens_v2_permissions:42:0xblock:0xtx:9:{EVENT_KIND_PERMISSION_CHANGED}:{resource}")
    );
    assert_eq!(event.namespace, "ens");
    assert_eq!(event.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(event.resource_id, Some(resource_id));
    assert_eq!(event.event_kind, EVENT_KIND_PERMISSION_CHANGED);
    assert_eq!(event.source_family, "ens_v2_resolver_l1");
    assert_eq!(event.manifest_version, 7);
    assert_eq!(event.source_manifest_id, Some(42));
    assert_eq!(event.chain_id.as_deref(), Some("sepolia-dev"));
    assert_eq!(event.block_number, Some(123));
    assert_eq!(event.block_hash.as_deref(), Some("0xblock"));
    assert_eq!(event.transaction_hash.as_deref(), Some("0xtx"));
    assert_eq!(event.log_index, Some(9));
    assert_eq!(event.derivation_kind, "ens_v2_permissions");
    assert_eq!(event.canonicality_state, CanonicalityState::Canonical);
    assert_eq!(
        event.raw_fact_ref,
        json!({
            "kind": "raw_log",
            "chain_id": "sepolia-dev",
            "block_hash": "0xblock",
            "block_number": 123,
            "transaction_hash": "0xtx",
            "transaction_index": 4,
            "log_index": 9,
            "emitting_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.before_state,
        json!({
            "subject": account,
            "role_bitmap": topic_word(0),
            "effective_powers": [],
        })
    );
    assert_eq!(event.after_state["subject"], json!(account));
    assert_eq!(
        event.after_state["scope"],
        json!({
            "kind": "resolver",
            "chain_id": "sepolia-dev",
            "resolver_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(
        event.after_state["grant_source"]["source_event"],
        "EACRolesChanged"
    );
    assert_eq!(
        event.after_state["grant_source"]["upstream_resource"],
        resource
    );
    assert_eq!(event.after_state["grant_source"]["root_resource"], false);
    assert_eq!(
        event.after_state["grant_source"]["changed_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(event.after_state["revocation_source"], json!(null));
    assert_eq!(event.after_state["inheritance_path"], json!([]));
    assert_eq!(
        event.after_state["selector"],
        json!({
            "kind": "name",
            "key": null,
            "hash": null,
            "normalized_name": "alice.eth",
            "dns_encoded_name": format!("0x{}", hex_string(&name)),
        })
    );

    Ok(())
}

fn raw_log(topics: Vec<String>, data: Vec<u8>) -> PermissionsRawLogRow {
    PermissionsRawLogRow {
        chain_id: "sepolia-dev".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 123,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 4,
        log_index: 9,
        emitting_address: "0x2222222222222222222222222222222222222222".to_owned(),
        emitting_contract_instance_id: Uuid::parse_str("22222222-2222-5222-8222-222222222222")
            .expect("test uuid should parse"),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 42,
        namespace: "ens".to_owned(),
        source_family: "ens_v2_resolver_l1".to_owned(),
        manifest_version: 7,
    }
}

fn dns_name(first: &str, second: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(first.len() as u8);
    bytes.extend_from_slice(first.as_bytes());
    bytes.push(second.len() as u8);
    bytes.extend_from_slice(second.as_bytes());
    bytes.push(0);
    bytes
}

fn encode_two_dynamic(first: &[u8], second: &[u8]) -> Vec<u8> {
    let first_tail = dynamic_tail(first);
    let second_tail = dynamic_tail(second);
    let second_offset = 64 + first_tail.len();
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(64));
    data.extend_from_slice(&hex_word(second_offset as u64));
    data.extend_from_slice(&first_tail);
    data.extend_from_slice(&second_tail);
    data
}

fn dynamic_tail(bytes: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(bytes.len() as u64));
    data.extend_from_slice(bytes);
    let padding = (32 - (bytes.len() % 32)) % 32;
    data.extend(std::iter::repeat_n(0, padding));
    data
}

fn address_topic(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

fn topic_word(value: u64) -> String {
    format!("0x{value:064x}")
}

fn hex_word(value: u64) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes.to_vec()
}

fn bitmap_with_last_byte(value: u8) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[31] = value;
    bytes.to_vec()
}

fn bitmap_hex_with_last_byte(value: u8) -> String {
    format!("0x{}", hex_string(bitmap_with_last_byte(value)))
}

fn test_permissions_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(HashMap::from([
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
        (
            ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
        ),
    ]))
}
