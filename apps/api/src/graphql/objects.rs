use async_graphql::{Object, SimpleObject};

/// Subgraph `Account` — the lowercased address as `id`.
#[derive(SimpleObject)]
#[graphql(name = "Account")]
pub(crate) struct Account {
    pub(crate) id: String,
}

/// Subgraph `AddressRecord` — a coin-typed address record. Dashboard scope never populates these,
/// but the type is declared so `Resolver.addresses` keeps its shape.
#[derive(SimpleObject)]
#[graphql(name = "AddressRecord")]
pub(crate) struct AddressRecord {
    #[graphql(name = "coinType")]
    pub(crate) coin_type: i32,
    pub(crate) address: String,
}

/// Subgraph `Resolver`. Record fields are stubbed for dashboard scope (`texts`/`addresses` empty,
/// `contentHash` null); `id`/`address` carry the resolver contract address. The profile page reads
/// the record fields — that is a planned fast-follow.
#[derive(SimpleObject)]
#[graphql(name = "Resolver")]
pub(crate) struct Resolver {
    pub(crate) id: String,
    pub(crate) address: Option<String>,
    pub(crate) texts: Option<Vec<String>>,
    #[graphql(name = "contentHash")]
    pub(crate) content_hash: Option<String>,
    pub(crate) addresses: Option<Vec<AddressRecord>>,
}

impl Resolver {
    pub(super) fn from_address(address: String) -> Self {
        Self {
            id: address.clone(),
            address: Some(address),
            texts: Some(Vec::new()),
            content_hash: None,
            addresses: Some(Vec::new()),
        }
    }
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
    pub(crate) created_at: Option<i32>,
    pub(crate) expiry_date: Option<i32>,
    pub(crate) resolver_address: Option<String>,
    pub(crate) owner_id: String,
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
    async fn created_at(&self) -> Option<i32> {
        self.created_at
    }

    #[graphql(name = "expiryDate")]
    async fn expiry_date(&self) -> Option<i32> {
        self.expiry_date
    }

    async fn resolver(&self) -> Option<Resolver> {
        self.resolver_address.clone().map(Resolver::from_address)
    }

    async fn owner(&self) -> Account {
        Account {
            id: self.owner_id.clone(),
        }
    }
}
