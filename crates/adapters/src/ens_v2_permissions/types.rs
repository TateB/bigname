use bigname_storage::CanonicalityState;
use sqlx::types::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PermissionsRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
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
pub(super) struct ResolverResourceHint {
    pub(super) upstream_resource: String,
    pub(super) logical_name_id: Option<String>,
    pub(super) normalized_name: Option<String>,
    pub(super) dns_encoded_name: Option<Vec<u8>>,
    pub(super) selector_kind: String,
    pub(super) selector_key: Option<String>,
    pub(super) selector_hash: Option<String>,
    pub(super) first_ref: PermissionRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PermissionRef {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) source_manifest_id: i64,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

pub(super) enum PermissionsObservation {
    NamedResource {
        resource: String,
        name: Vec<u8>,
    },
    NamedTextResource {
        resource: String,
        name: Vec<u8>,
        key_hash: String,
        key: String,
    },
    NamedAddrResource {
        resource: String,
        name: Vec<u8>,
        coin_type: String,
    },
    EacRolesChanged {
        resource: String,
        account: String,
        old_role_bitmap: String,
        new_role_bitmap: String,
    },
}

impl PermissionsRawLogRow {
    pub(super) fn reference(&self) -> PermissionRef {
        PermissionRef {
            chain_id: self.chain_id.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            emitting_address: self.emitting_address.clone(),
            emitting_contract_instance_id: self.emitting_contract_instance_id,
            canonicality_state: self.canonicality_state,
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
    }
}
