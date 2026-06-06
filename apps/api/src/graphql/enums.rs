use async_graphql::Enum;

/// Subgraph `Domain_orderBy`. The underscore + lowercase-`o` type name and the lowercase-camel
/// value names are pinned by the Manager's committed codegen, so they are set explicitly rather
/// than relying on async-graphql's default SCREAMING_SNAKE_CASE rename.
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "Domain_orderBy")]
pub(crate) enum DomainOrderBy {
    #[graphql(name = "createdAt")]
    CreatedAt,
    #[graphql(name = "expiryDate")]
    ExpiryDate,
    /// No storage sort column. The dashboard never sends it; declared for the codegen contract and
    /// mapped to a degenerate (name) sort in the resolver.
    #[graphql(name = "id")]
    Id,
    #[graphql(name = "name")]
    Name,
    /// Degenerate on Sepolia v2 — no producer writes `registration_date`, so the column is NULL.
    #[graphql(name = "registrationDate")]
    RegistrationDate,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "OrderDirection")]
pub(crate) enum OrderDirection {
    #[graphql(name = "asc")]
    Asc,
    #[graphql(name = "desc")]
    Desc,
}
