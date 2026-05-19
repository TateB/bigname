use crate::{
    address_names::AddressNameRelation, name_current::NameCurrentRow,
    primary_name::PrimaryNameClaimStatus, record_inventory::RecordInventoryCurrentRow,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityNameRecordRow {
    pub row: NameCurrentRow,
    pub record_inventory_current: Option<RecordInventoryCurrentRow>,
    pub relations: Vec<IdentityAddressRelationRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityAddressRelationRow {
    pub address: String,
    pub logical_name_id: String,
    pub relation: AddressNameRelation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReverseIdentityRoles {
    Owned,
    Managed,
    Both,
}

impl ReverseIdentityRoles {
    pub fn includes(self, relation: AddressNameRelation) -> bool {
        match self {
            Self::Owned => matches!(
                relation,
                AddressNameRelation::Registrant | AddressNameRelation::TokenHolder
            ),
            Self::Managed => matches!(relation, AddressNameRelation::EffectiveController),
            Self::Both => true,
        }
    }

    pub(super) fn storage_value(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::Managed => "managed",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityStorageInput {
    pub address: String,
    pub coin_type: String,
    pub roles: ReverseIdentityRoles,
    pub page_size: i64,
    pub cursor: Option<ReverseIdentityCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityCursor {
    pub is_primary: bool,
    pub role_rank: i16,
    pub normalized_name: String,
    pub namespace: String,
    pub namehash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityGroup {
    pub input: ReverseIdentityStorageInput,
    pub entries: Vec<ReverseIdentityRecordRow>,
    pub total_count: Option<u64>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityRecordRow {
    pub name_record: IdentityNameRecordRow,
    pub relation_facets: Vec<AddressNameRelation>,
    pub primary_name: Option<IdentityPrimaryNameSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityPrimaryNameSnapshot {
    pub address: String,
    pub namespace: String,
    pub coin_type: String,
    pub claim_status: PrimaryNameClaimStatus,
    pub normalized_claim_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexingStatusRead {
    pub chains: Vec<IndexingStatusChainRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexingStatusChainRow {
    pub chain_id: String,
    pub canonical_block: Option<i64>,
    pub safe_block: Option<i64>,
    pub finalized_block: Option<i64>,
    pub canonical_timestamp: Option<sqlx::types::time::OffsetDateTime>,
    pub latest_projected_block: Option<i64>,
    pub latest_projected_timestamp: Option<sqlx::types::time::OffsetDateTime>,
}
