use async_graphql::{ID, InputObject, MaybeUndefined};

use super::scalars::Bytes;

/// Subgraph block constraint. The endpoint currently accepts constraints satisfied by the indexed
/// state eligible for reads; it does not execute queries against historical data.
#[derive(Clone, Default, InputObject)]
#[graphql(name = "Block_height")]
pub(crate) struct BlockHeight {
    pub(crate) hash: MaybeUndefined<Bytes>,
    pub(crate) number: MaybeUndefined<i32>,
    #[graphql(name = "number_gte")]
    pub(crate) number_gte: MaybeUndefined<i32>,
}

/// Subgraph `DomainFilter`. Field names that are snake_case in the subgraph schema (`owner_in`,
/// `name_contains`) are pinned explicitly; async-graphql would otherwise camelCase them. Only
/// `owner`, `owner_in`, `name`, `name_contains`, and `isMigrated` affect storage filters; the rest
/// are declared for compatibility with subgraph-shaped variables.
#[derive(InputObject, Default)]
#[graphql(name = "DomainFilter")]
pub(crate) struct DomainFilter {
    pub(crate) id: Option<String>,
    pub(crate) owner: Option<String>,
    #[graphql(name = "owner_in")]
    pub(crate) owner_in: Option<Vec<String>>,
    pub(crate) name: Option<String>,
    #[graphql(name = "name_contains")]
    pub(crate) name_contains: Option<String>,
    #[graphql(name = "isMigrated")]
    pub(crate) is_migrated: Option<bool>,
}

/// Generated-style partial `Domain_filter` for `Query.domains`.
#[derive(InputObject, Default)]
#[graphql(name = "Domain_filter")]
pub(crate) struct DomainEntityFilter {
    pub(crate) id: MaybeUndefined<ID>,
    #[graphql(name = "id_not")]
    pub(crate) id_not: MaybeUndefined<ID>,
    #[graphql(name = "id_gt")]
    pub(crate) id_gt: MaybeUndefined<ID>,
    #[graphql(name = "id_gte")]
    pub(crate) id_gte: MaybeUndefined<ID>,
    #[graphql(name = "id_lt")]
    pub(crate) id_lt: MaybeUndefined<ID>,
    #[graphql(name = "id_lte")]
    pub(crate) id_lte: MaybeUndefined<ID>,
    #[graphql(name = "id_in")]
    pub(crate) id_in: MaybeUndefined<Vec<ID>>,
    #[graphql(name = "id_not_in")]
    pub(crate) id_not_in: MaybeUndefined<Vec<ID>>,
    pub(crate) owner: Option<String>,
    #[graphql(name = "owner_not")]
    pub(crate) owner_not: MaybeUndefined<String>,
    #[graphql(name = "owner_gt")]
    pub(crate) owner_gt: MaybeUndefined<String>,
    #[graphql(name = "owner_gte")]
    pub(crate) owner_gte: MaybeUndefined<String>,
    #[graphql(name = "owner_lt")]
    pub(crate) owner_lt: MaybeUndefined<String>,
    #[graphql(name = "owner_lte")]
    pub(crate) owner_lte: MaybeUndefined<String>,
    #[graphql(name = "owner_in")]
    pub(crate) owner_in: Option<Vec<String>>,
    #[graphql(name = "owner_not_in")]
    pub(crate) owner_not_in: MaybeUndefined<Vec<String>>,
    #[graphql(name = "owner_contains")]
    pub(crate) owner_contains: MaybeUndefined<String>,
    #[graphql(name = "owner_contains_nocase")]
    pub(crate) owner_contains_nocase: MaybeUndefined<String>,
    #[graphql(name = "owner_not_contains")]
    pub(crate) owner_not_contains: MaybeUndefined<String>,
    #[graphql(name = "owner_not_contains_nocase")]
    pub(crate) owner_not_contains_nocase: MaybeUndefined<String>,
    #[graphql(name = "owner_starts_with")]
    pub(crate) owner_starts_with: MaybeUndefined<String>,
    #[graphql(name = "owner_starts_with_nocase")]
    pub(crate) owner_starts_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "owner_not_starts_with")]
    pub(crate) owner_not_starts_with: MaybeUndefined<String>,
    #[graphql(name = "owner_not_starts_with_nocase")]
    pub(crate) owner_not_starts_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "owner_ends_with")]
    pub(crate) owner_ends_with: MaybeUndefined<String>,
    #[graphql(name = "owner_ends_with_nocase")]
    pub(crate) owner_ends_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "owner_not_ends_with")]
    pub(crate) owner_not_ends_with: MaybeUndefined<String>,
    #[graphql(name = "owner_not_ends_with_nocase")]
    pub(crate) owner_not_ends_with_nocase: MaybeUndefined<String>,
    pub(crate) name: MaybeUndefined<String>,
    #[graphql(name = "name_not")]
    pub(crate) name_not: MaybeUndefined<String>,
    #[graphql(name = "name_gt")]
    pub(crate) name_gt: MaybeUndefined<String>,
    #[graphql(name = "name_gte")]
    pub(crate) name_gte: MaybeUndefined<String>,
    #[graphql(name = "name_lt")]
    pub(crate) name_lt: MaybeUndefined<String>,
    #[graphql(name = "name_lte")]
    pub(crate) name_lte: MaybeUndefined<String>,
    #[graphql(name = "name_in")]
    pub(crate) name_in: MaybeUndefined<Vec<String>>,
    #[graphql(name = "name_not_in")]
    pub(crate) name_not_in: MaybeUndefined<Vec<String>>,
    #[graphql(name = "name_contains")]
    pub(crate) name_contains: MaybeUndefined<String>,
    #[graphql(name = "name_contains_nocase")]
    pub(crate) name_contains_nocase: MaybeUndefined<String>,
    #[graphql(name = "name_not_contains")]
    pub(crate) name_not_contains: MaybeUndefined<String>,
    #[graphql(name = "name_not_contains_nocase")]
    pub(crate) name_not_contains_nocase: MaybeUndefined<String>,
    #[graphql(name = "name_starts_with")]
    pub(crate) name_starts_with: MaybeUndefined<String>,
    #[graphql(name = "name_starts_with_nocase")]
    pub(crate) name_starts_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "name_not_starts_with")]
    pub(crate) name_not_starts_with: MaybeUndefined<String>,
    #[graphql(name = "name_not_starts_with_nocase")]
    pub(crate) name_not_starts_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "name_ends_with")]
    pub(crate) name_ends_with: MaybeUndefined<String>,
    #[graphql(name = "name_ends_with_nocase")]
    pub(crate) name_ends_with_nocase: MaybeUndefined<String>,
    #[graphql(name = "name_not_ends_with")]
    pub(crate) name_not_ends_with: MaybeUndefined<String>,
    #[graphql(name = "name_not_ends_with_nocase")]
    pub(crate) name_not_ends_with_nocase: MaybeUndefined<String>,
}

#[derive(InputObject, Default)]
#[graphql(name = "Account_filter")]
pub(crate) struct AccountEntityFilter {
    pub(crate) id: Option<ID>,
    #[graphql(name = "id_in")]
    pub(crate) id_in: Option<Vec<ID>>,
}

#[derive(InputObject, Default)]
#[graphql(name = "Resolver_filter")]
pub(crate) struct ResolverEntityFilter {
    pub(crate) id: Option<ID>,
    pub(crate) address: Option<Bytes>,
    pub(crate) domain: Option<String>,
}

/// Subgraph `RegistrationFilter`.
#[derive(InputObject, Default)]
#[graphql(name = "RegistrationFilter")]
pub(crate) struct RegistrationFilter {
    pub(crate) registrant: Option<String>,
    #[graphql(name = "registrant_in")]
    pub(crate) registrant_in: Option<Vec<String>>,
}
