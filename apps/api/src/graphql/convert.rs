use bigname_storage::NameCurrentListRow;
use sqlx::types::time::OffsetDateTime;

use super::objects::Domain;

/// Non-null `owner` fallback for ownerless names (all-zero address).
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Mirrors the REST row→fields mapping (`responses/app_facing/names_collection.rs`) so GraphQL and
/// REST agree on the derived `owner`/`tokenId`/dates/`resolver`. `owner` resolves the non-null
/// `Account!` fallback chain here so the resolver stays trivial.
impl From<NameCurrentListRow> for Domain {
    fn from(row: NameCurrentListRow) -> Self {
        let owner_id = non_empty(row.owner)
            .or_else(|| non_empty(row.registrant))
            .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
        Self {
            id: row.row.namehash,
            name: Some(row.row.canonical_display_name),
            normalized_name: Some(row.row.normalized_name),
            token_id: non_empty(row.token_id),
            created_at: row.created_at.map(unix_seconds_i32),
            expiry_date: row.expiry_date.map(unix_seconds_i32),
            resolver_address: non_empty(row.resolver_address),
            owner_id,
        }
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

/// Subgraph `createdAt`/`expiryDate` are codegen-pinned `Int`. Saturating to `i32::MAX` keeps the
/// dashboard rendering for the Sepolia test scope; far-future (post-2038) expiries would need a
/// wider Manager scalar — out of scope per the plan.
fn unix_seconds_i32(timestamp: OffsetDateTime) -> i32 {
    i32::try_from(timestamp.unix_timestamp()).unwrap_or(i32::MAX)
}
