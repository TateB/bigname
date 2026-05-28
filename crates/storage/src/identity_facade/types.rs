use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{address_names::AddressNameRelation, primary_name::PrimaryNameClaimStatus};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityNameRecordRow {
    pub row: IdentityNameCurrentRow,
    pub record_inventory_current: Option<IdentityRecordInventoryRow>,
    pub relations: Vec<IdentityAddressRelationRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityNameCurrentRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub labelhash_count: Option<i32>,
    pub resource_id: Option<Uuid>,
    pub record_inventory_boundary_key: Option<String>,
    pub declared_summary: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub last_recomputed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityRecordInventoryRow {
    pub resource_id: Uuid,
    pub entries: Value,
    pub unsupported_families: Value,
    pub chain_positions: Value,
    pub last_recomputed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityAddressRelationRow {
    pub address: String,
    pub logical_name_id: String,
    pub relation: AddressNameRelation,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
pub struct ReverseIdentityFeedInput {
    pub address: String,
    pub coin_type: String,
    pub roles: ReverseIdentityRoles,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityFeedGroup {
    pub input: ReverseIdentityFeedInput,
    pub record: Option<ReverseIdentityFeedRecordRow>,
    pub total_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityFeedRecordRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub chain_positions: Value,
    pub coverage: Value,
    pub is_primary: bool,
    pub relation_facets: Vec<AddressNameRelation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseIdentityRecordRow {
    pub name_record: IdentityNameRecordRow,
    pub relation_facets: Vec<AddressNameRelation>,
    pub primary_name: Option<IdentityPrimaryNameSnapshot>,
    pub requested_coin_type: String,
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
    pub has_unscoped_pending_invalidations: bool,
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
