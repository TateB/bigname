use async_graphql::dataloader::DataLoader;
use async_graphql::{Context, Object, Result, SimpleObject};
use sqlx::types::Uuid;

use super::convert::resolver_from_store;
use super::error::internal_error;
use super::loader::{RecordInventoryLoader, record_inventory_key};

/// Subgraph `Account` — the lowercased address as `id`.
#[derive(SimpleObject)]
#[graphql(name = "Account")]
pub(crate) struct Account {
    pub(crate) id: String,
}

/// Subgraph `AddressRecord` — a coin-typed address record. `coinType` is `u32`, not `i32`:
/// ENSIP-11 EVM coin types set the top bit (`0x80000000 | chainId`, e.g. `2147483658`), which the
/// reference endpoint also serves beyond the signed-32-bit range.
#[derive(SimpleObject)]
#[graphql(name = "AddressRecord")]
pub(crate) struct AddressRecord {
    #[graphql(name = "coinType")]
    pub(crate) coin_type: u32,
    pub(crate) address: String,
}

/// Subgraph `Resolver`. `id`/`address` carry the resolver contract address (non-null, matching the
/// Manager codegen's `address: string`); the record fields (`texts`/`contentHash`/`addresses`) are
/// read from the `record_inventory_current` projection by [`resolver_from_store`] — a name whose
/// resolver has no projected records serves the empty shapes.
#[derive(SimpleObject)]
#[graphql(name = "Resolver")]
pub(crate) struct Resolver {
    pub(crate) id: String,
    pub(crate) address: String,
    pub(crate) texts: Option<Vec<String>>,
    #[graphql(name = "contentHash")]
    pub(crate) content_hash: Option<String>,
    pub(crate) addresses: Option<Vec<AddressRecord>>,
}

/// Subgraph `DomainConnection` — only `totalCount` is exercised (`MigratedNamesCount`).
#[derive(SimpleObject)]
#[graphql(name = "DomainConnection")]
pub(crate) struct DomainConnection {
    #[graphql(name = "totalCount")]
    pub(crate) total_count: Option<i32>,
}

/// Subgraph `RegistrationConnection` — only `totalCount` is exercised (`OwnedNamesCount`).
#[derive(SimpleObject)]
#[graphql(name = "RegistrationConnection")]
pub(crate) struct RegistrationConnection {
    #[graphql(name = "totalCount")]
    pub(crate) total_count: Option<i32>,
}

/// Subgraph `Domain`. A manual `#[Object]` (not `SimpleObject`) so `owner` is non-null `Account!`;
/// the storage fallback (`owner → registrant → zero-address`) is resolved in `convert.rs`.
pub(crate) struct Domain {
    pub(crate) id: String,
    pub(crate) name: Option<String>,
    pub(crate) normalized_name: Option<String>,
    pub(crate) token_id: Option<String>,
    pub(crate) created_at: i32,
    pub(crate) expiry_date: Option<i32>,
    pub(crate) resolver_address: Option<String>,
    pub(crate) owner_id: String,
    /// `(resource_id, record_version_boundary)` for the name's `record_inventory_current` row,
    /// derived in `convert.rs`; `None` when the row carries no resolvable boundary, in which case
    /// the resolver serves the empty record shapes without a read.
    pub(crate) record_inventory_key: Option<(Uuid, serde_json::Value)>,
}

#[Object]
impl Domain {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[graphql(name = "normalizedName")]
    async fn normalized_name(&self) -> Option<&str> {
        self.normalized_name.as_deref()
    }

    #[graphql(name = "tokenId")]
    async fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> i32 {
        self.created_at
    }

    #[graphql(name = "expiryDate")]
    async fn expiry_date(&self) -> Option<i32> {
        self.expiry_date
    }

    /// The page's `resolver` reads are coalesced through a DataLoader, so a list of N domains costs
    /// one batched `record_inventory_current` query (plus a fallback only for rare exact-key misses)
    /// rather than N point reads. Names without a projected inventory row (the common case until the
    /// resolver-log sweep lands) serve the empty record shapes without contributing a key.
    async fn resolver(&self, ctx: &Context<'_>) -> Result<Option<Resolver>> {
        let Some(address) = self.resolver_address.clone() else {
            return Ok(None);
        };
        let inventory = match self.record_inventory_key.as_ref() {
            Some((resource_id, boundary)) => {
                let loader = ctx.data::<DataLoader<RecordInventoryLoader>>()?;
                loader
                    .load_one(record_inventory_key(*resource_id, boundary))
                    .await
                    .map_err(|error| {
                        // `{error:#}` keeps the storage layer's full anyhow cause chain in the log
                        // (plain `{error}` would flatten it to just the outermost message).
                        internal_error(
                            "Domain.resolver",
                            anyhow::anyhow!("record inventory batch load failed: {error:#}"),
                        )
                    })?
            }
            None => None,
        };
        Ok(Some(resolver_from_store(address, inventory.as_ref())))
    }

    async fn owner(&self) -> Account {
        Account {
            id: self.owner_id.clone(),
        }
    }
}
