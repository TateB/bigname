use super::*;
use alloy_sol_types::{SolEvent, sol};

sol! {
    #[derive(Debug)]
    event ABIChanged(bytes32 indexed node, uint256 indexed contentType);

    #[derive(Debug)]
    event ContentChanged(bytes32 indexed node, bytes32 hash);

    #[derive(Debug)]
    event ContenthashChanged(bytes32 indexed node, bytes hash);

    #[derive(Debug)]
    event DNSRecordChanged(bytes32 indexed node, bytes name, uint16 resource, bytes record);

    #[derive(Debug)]
    event DNSRecordDeleted(bytes32 indexed node, bytes name, uint16 resource);

    #[derive(Debug)]
    event DNSZonehashChanged(bytes32 indexed node, bytes lastzonehash, bytes zonehash);

    #[derive(Debug)]
    event InterfaceChanged(bytes32 indexed node, bytes4 indexed interfaceID, address implementer);

    #[derive(Debug)]
    event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData);
}

pub(super) fn build_ens_v1_generic_record_observation(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<AuthorityObservation>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        return Ok(None);
    }

    if event_topics.matches(ABI_CHANGED_SIGNATURE, topic0)? {
        return abi_changed_observation(raw_log);
    }

    if event_topics.matches(CONTENT_CHANGED_SIGNATURE, topic0)? {
        return content_changed_observation(raw_log);
    }

    if event_topics.matches(CONTENTHASH_CHANGED_SIGNATURE, topic0)? {
        return contenthash_changed_observation(raw_log);
    }

    if event_topics.matches(DNS_RECORD_CHANGED_SIGNATURE, topic0)? {
        return dns_record_changed_observation(raw_log);
    }

    if event_topics.matches(DNS_RECORD_DELETED_SIGNATURE, topic0)? {
        return dns_record_deleted_observation(raw_log);
    }

    if event_topics.matches(DNS_ZONEHASH_CHANGED_SIGNATURE, topic0)? {
        return dns_zonehash_changed_observation(raw_log);
    }

    if event_topics.matches(INTERFACE_CHANGED_SIGNATURE, topic0)? {
        return interface_changed_observation(raw_log);
    }

    if event_topics.matches(DATA_CHANGED_SIGNATURE, topic0)? {
        return data_changed_observation(raw_log);
    }

    Ok(None)
}

fn abi_changed_observation(raw_log: &AuthorityRawLogRow) -> Result<Option<AuthorityObservation>> {
    let Some(event) = decode_event_skip::<ABIChanged>(raw_log, "ABIChanged log is malformed")
    else {
        return Ok(None);
    };
    let Ok(content_type) = crate::evm_abi::u256_i64(event.contentType, "ABIChanged content type")
    else {
        return Ok(None);
    };
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: format!("abi:{content_type}"),
            record_family: "abi".to_owned(),
            selector_key: Some(content_type.to_string()),
        },
        Some(json!(content_type)),
    )
}

fn content_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<ContentChanged>(raw_log, "ContentChanged log is malformed")
    else {
        return Ok(None);
    };
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: "content".to_owned(),
            record_family: "content".to_owned(),
            selector_key: None,
        },
        Some(json!(hex_string(event.hash.as_slice()))),
    )
}

fn contenthash_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<ContenthashChanged>(raw_log, "ContenthashChanged log is malformed")
    else {
        return Ok(None);
    };
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: "contenthash".to_owned(),
            record_family: "contenthash".to_owned(),
            selector_key: None,
        },
        Some(json!({
            "encoding": "hex",
            "bytes": hex_string(event.hash.as_ref()),
        })),
    )
}

fn dns_record_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<DNSRecordChanged>(raw_log, "DNSRecordChanged log is malformed")
    else {
        return Ok(None);
    };
    let resource = i64::from(event.resource);
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        dns_record_selector(resource, event.name.as_ref()),
        Some(json!({
            "encoding": "hex",
            "bytes": hex_string(event.record.as_ref()),
        })),
    )
}

fn dns_record_deleted_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<DNSRecordDeleted>(raw_log, "DNSRecordDeleted log is malformed")
    else {
        return Ok(None);
    };
    let resource = i64::from(event.resource);
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        dns_record_selector(resource, event.name.as_ref()),
        Some(json!({ "deleted": true })),
    )
}

fn dns_zonehash_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<DNSZonehashChanged>(raw_log, "DNSZonehashChanged log is malformed")
    else {
        return Ok(None);
    };
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: "dns:zonehash".to_owned(),
            record_family: "dns".to_owned(),
            selector_key: Some("zonehash".to_owned()),
        },
        Some(json!({
            "previous": {
                "encoding": "hex",
                "bytes": hex_string(event.lastzonehash.as_ref()),
            },
            "current": {
                "encoding": "hex",
                "bytes": hex_string(event.zonehash.as_ref()),
            },
        })),
    )
}

fn interface_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(event) =
        decode_event_skip::<InterfaceChanged>(raw_log, "InterfaceChanged log is malformed")
    else {
        return Ok(None);
    };
    let interface_id = hex_string(event.interfaceID.as_slice());
    let implementer = crate::evm_abi::address_hex(event.implementer);
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: format!("interface:{interface_id}"),
            record_family: "interface".to_owned(),
            selector_key: Some(interface_id),
        },
        Some(json!(implementer)),
    )
}

fn data_changed_observation(raw_log: &AuthorityRawLogRow) -> Result<Option<AuthorityObservation>> {
    let Some(event) = decode_event_skip::<DataChanged>(raw_log, "DataChanged log is malformed")
    else {
        return Ok(None);
    };
    if hex_string(event.indexedKey.as_slice()) != keccak256_hex(event.key.as_bytes()) {
        return Ok(None);
    }
    let indexed_data_hash = hex_string(event.indexedData.as_slice());
    resolver_record_observation(
        raw_log,
        hex_string(event.node.as_slice()),
        RecordSelector {
            record_key: format!("data:{}", event.key),
            record_family: "data".to_owned(),
            selector_key: Some(event.key),
        },
        Some(json!({ "indexed_data_hash": indexed_data_hash })),
    )
}

fn resolver_record_observation(
    raw_log: &AuthorityRawLogRow,
    namehash: String,
    selector: RecordSelector,
    value: Option<Value>,
) -> Result<Option<AuthorityObservation>> {
    Ok(Some(AuthorityObservation::RecordChanged(
        RecordChangeObservation {
            namehash,
            resolver: raw_log.emitting_address.clone(),
            selector,
            value,
            raw_name: None,
            reference: raw_log.reference(),
        },
    )))
}

fn dns_record_selector(resource: i64, dns_name: &[u8]) -> RecordSelector {
    let selector_key = format!("{resource}:{}", hex_string(dns_name));
    RecordSelector {
        record_key: format!("dns:{selector_key}"),
        record_family: "dns".to_owned(),
        selector_key: Some(selector_key),
    }
}

fn decode_event_skip<E>(raw_log: &AuthorityRawLogRow, context: &'static str) -> Option<E>
where
    E: SolEvent,
{
    crate::evm_abi::decode_event_log::<E>(&raw_log.topics, &raw_log.data, context).ok()
}
