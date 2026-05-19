use bigname_storage::CanonicalityState;
use sqlx::types::{Uuid, time::OffsetDateTime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ResolverRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) event_position_timestamp: OffsetDateTime,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NameLink {
    pub(super) logical_name_id: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) normalized_name: Option<String>,
    pub(super) canonical_display_name: Option<String>,
    pub(super) namehash: Option<String>,
}

impl NameLink {
    pub(super) fn unknown() -> Self {
        Self {
            logical_name_id: None,
            resource_id: None,
            normalized_name: None,
            canonical_display_name: None,
            namehash: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreimageObservation {
    pub(super) dns_encoded_name: String,
    pub(super) decoded_name: Option<String>,
    pub(super) labelhashes: Vec<String>,
    pub(super) namehash: String,
}

pub(super) enum ResolverObservation {
    AddressChanged {
        node: String,
        coin_type: String,
        address_bytes: Vec<u8>,
    },
    TextChanged {
        node: String,
        key: String,
        value: String,
    },
    ContenthashChanged {
        node: String,
        hash: Vec<u8>,
    },
    NameChanged {
        node: String,
        name: String,
    },
    VersionChanged {
        node: String,
        version: i64,
    },
    AliasChanged {
        from_name: Vec<u8>,
        to_name: Vec<u8>,
    },
    NamedResource {
        name: Vec<u8>,
    },
    NamedTextResource {
        name: Vec<u8>,
    },
    NamedAddrResource {
        name: Vec<u8>,
    },
}
