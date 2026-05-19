use alloy_primitives::{hex, keccak256};

pub(crate) const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
pub(crate) const GENERIC_SOURCE_SCOPE_ADDRESS: &str = "*";

const GENERIC_RESOLVER_RECORD_EVENT_SIGNATURES: &[&str] = &[
    "ABIChanged(bytes32,uint256)",
    "AddrChanged(bytes32,address)",
    "AddressChanged(bytes32,uint256,bytes)",
    "ContentChanged(bytes32,bytes32)",
    "ContenthashChanged(bytes32,bytes)",
    "DNSRecordChanged(bytes32,bytes,uint16,bytes)",
    "DNSRecordDeleted(bytes32,bytes,uint16)",
    "DNSZonehashChanged(bytes32,bytes,bytes)",
    "DataChanged(bytes32,string,string,bytes)",
    "InterfaceChanged(bytes32,bytes4,address)",
    "NameChanged(bytes32,string)",
    "TextChanged(bytes32,string,string)",
    "TextChanged(bytes32,string,string,string)",
    "VersionChanged(bytes32,uint64)",
];

pub(crate) fn generic_resolver_record_topic0s() -> Vec<String> {
    GENERIC_RESOLVER_RECORD_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("0x{}", hex::encode(keccak256(signature.as_bytes()))))
        .collect()
}
