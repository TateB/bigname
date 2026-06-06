use async_graphql::{Context, Object, Result};
use bigname_storage::{
    AddressNameRelation, NameCurrentAddressFilter, NameCurrentAddressRelationFilter,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListSort, count_name_current_list,
    load_name_current_list_page_offset, load_name_current_list_row_by_namehash,
};

use crate::state::AppState;

use super::enums::{DomainOrderBy, OrderDirection};
use super::error::internal_error;
use super::inputs::{DomainFilter, RegistrationFilter};
use super::objects::{Domain, DomainConnection, RegistrationConnection};

/// ENS is the Manager's only namespace (Sepolia v2).
const NAMESPACE: &str = "ens";
/// Page size for `domains` when the subgraph `first` argument is omitted.
const DEFAULT_DOMAINS_PAGE_SIZE: u64 = 100;

pub(crate) struct QueryRoot;

#[Object]
impl QueryRoot {
    /// `domain(id: String!)` — `id` is the EIP-137 namehash.
    async fn domain(&self, ctx: &Context<'_>, id: String) -> Result<Option<Domain>> {
        let state = ctx.data::<AppState>()?;
        let row = load_name_current_list_row_by_namehash(&state.pool, &id)
            .await
            .map_err(|error| internal_error("domain", error))?;
        Ok(row.map(Domain::from))
    }

    /// `domains(where, first, skip, orderBy, orderDirection)` — offset-paged list.
    async fn domains(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "where")] filter: Option<DomainFilter>,
        first: Option<i32>,
        skip: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<DomainOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
    ) -> Result<Vec<Domain>> {
        let limit = match first {
            Some(first) if first <= 0 => return Ok(Vec::new()),
            Some(first) => first as u64,
            None => DEFAULT_DOMAINS_PAGE_SIZE,
        };
        let offset = skip.unwrap_or(0).max(0) as u64;
        let (sort, order) = storage_sort(order_by, order_direction);
        let state = ctx.data::<AppState>()?;
        let rows = load_name_current_list_page_offset(
            &state.pool,
            &domain_filter_to_storage(filter),
            sort,
            order,
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("domains", error))?;
        Ok(rows.into_iter().map(Domain::from).collect())
    }

    /// `registrationConnection(first: 0, where) { totalCount }` — backs `OwnedNamesCount`.
    #[graphql(name = "registrationConnection")]
    async fn registration_connection(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "first")] _first: Option<i32>,
        #[graphql(name = "where")] filter: Option<RegistrationFilter>,
    ) -> Result<RegistrationConnection> {
        let filter = filter.unwrap_or_default();
        let storage_filter = NameCurrentListFilter {
            namespace: Some(NAMESPACE.to_owned()),
            address: address_membership(
                filter.registrant,
                filter.registrant_in,
                AddressNameRelation::Registrant,
            ),
            ..Default::default()
        };
        let state = ctx.data::<AppState>()?;
        let count = count_name_current_list(&state.pool, &storage_filter)
            .await
            .map_err(|error| internal_error("registrationConnection", error))?;
        Ok(RegistrationConnection {
            total_count: Some(count_to_i32(count)),
        })
    }

    /// `domainConnection(first: 0, where) { totalCount }` — backs `MigratedNamesCount`.
    #[graphql(name = "domainConnection")]
    async fn domain_connection(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "first")] _first: Option<i32>,
        #[graphql(name = "where")] filter: Option<DomainFilter>,
    ) -> Result<DomainConnection> {
        let state = ctx.data::<AppState>()?;
        let count = count_name_current_list(&state.pool, &domain_filter_to_storage(filter))
            .await
            .map_err(|error| internal_error("domainConnection", error))?;
        Ok(DomainConnection {
            total_count: Some(count_to_i32(count)),
        })
    }
}

fn storage_sort(
    order_by: Option<DomainOrderBy>,
    order_direction: Option<OrderDirection>,
) -> (NameCurrentListSort, NameCurrentListOrder) {
    let sort = match order_by.unwrap_or(DomainOrderBy::Name) {
        DomainOrderBy::CreatedAt => NameCurrentListSort::CreatedAt,
        DomainOrderBy::ExpiryDate => NameCurrentListSort::ExpiryDate,
        DomainOrderBy::RegistrationDate => NameCurrentListSort::RegistrationDate,
        // `id` has no storage sort column and the dashboard never sends it; map to the name sort.
        DomainOrderBy::Id | DomainOrderBy::Name => NameCurrentListSort::Name,
    };
    let order = match order_direction.unwrap_or(OrderDirection::Asc) {
        OrderDirection::Asc => NameCurrentListOrder::Asc,
        OrderDirection::Desc => NameCurrentListOrder::Desc,
    };
    (sort, order)
}

fn domain_filter_to_storage(filter: Option<DomainFilter>) -> NameCurrentListFilter {
    let filter = filter.unwrap_or_default();
    NameCurrentListFilter {
        namespace: Some(NAMESPACE.to_owned()),
        name: filter.name,
        contains: filter.name_contains,
        address: address_membership(
            filter.owner,
            filter.owner_in,
            AddressNameRelation::TokenHolder,
        ),
        is_migrated: filter.is_migrated,
        ..Default::default()
    }
}

/// Build a storage address-membership filter from a single address and/or an address list, under a
/// fixed relation. A non-empty list takes precedence (subgraph `owner_in`/`registrant_in`);
/// addresses are lowercased to match the stored `address_names_current` convention.
fn address_membership(
    single: Option<String>,
    many: Option<Vec<String>>,
    relation: AddressNameRelation,
) -> Option<NameCurrentAddressFilter> {
    let relation = NameCurrentAddressRelationFilter::Relation(relation);
    match many {
        Some(many) if !many.is_empty() => {
            let many: Vec<String> = many.into_iter().map(|a| a.to_lowercase()).collect();
            Some(NameCurrentAddressFilter {
                address: many[0].clone(),
                relation,
                addresses: Some(many),
            })
        }
        _ => single.map(|address| NameCurrentAddressFilter {
            address: address.to_lowercase(),
            relation,
            addresses: None,
        }),
    }
}

/// Subgraph `totalCount` is a codegen-pinned `Int`; saturate the storage `u64` count (absurdly
/// large for the Sepolia scope) into `i32`.
fn count_to_i32(count: u64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}
