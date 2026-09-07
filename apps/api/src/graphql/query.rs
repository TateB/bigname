use async_graphql::{Context, ID, MaybeUndefined, Object, Result};
use bigname_storage::{
    AddressNameRelation, NameCurrentAddressFilter, NameCurrentAddressRelationFilter,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListSort,
};

use crate::state::AppState;

use super::account_queries::{
    account_entity_filter_to_storage, load_phase_graphql_account_page_offset, resolve_account,
};
use super::effective_owner_filter::domain_effective_owner_filter;
use super::enums::{
    AccountOrderBy, DomainOrderBy, OrderDirection, ResolverOrderBy, SubgraphErrorPolicy,
    generated_order,
};
use super::error::internal_error;
use super::generated_filter_ops::{GeneratedDomainFilter, IdFilter, StringFilter};
use super::inputs::{
    AccountEntityFilter, BlockHeight, DomainEntityFilter, DomainFilter, RegistrationFilter,
    ResolverEntityFilter,
};
use super::meta::{SubgraphMeta, resolve_meta};
use super::name_queries::{
    GeneratedDomainSort, count_phase_graphql_name_list, load_phase_graphql_name_list_page_offset,
    load_phase_graphql_name_row_by_name, load_phase_graphql_name_row_by_namehash,
};
use super::objects::{Account, Domain, DomainConnection, RegistrationConnection, Resolver};
use super::resolver_queries::{
    hydrate_resolver_rows, load_phase_graphql_resolver_page_offset, resolve_resolver,
    resolver_entity_filter_to_storage,
};
use super::snapshot::{
    graphql_snapshot_chain_ids, load_graphql_entity_head, load_graphql_head,
    require_account_rows_at_head, require_count_at_head, require_resolver_rows_at_head,
    require_rows_at_head, revalidate_graphql_head,
};

const NAMESPACE: &str = "ens";
const DEFAULT_DOMAINS_PAGE_SIZE: u64 = 100;
const MAX_DOMAINS_PAGE_SIZE: u64 = crate::v2::MAX_PAGE_SIZE;
const MAX_DOMAINS_SKIP: u64 = 1_000_000;

pub(crate) struct Query;

#[Object]
impl Query {
    async fn account(
        &self,
        ctx: &Context<'_>,
        id: ID,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Option<Account> {
        match resolve_account(ctx, id, block.as_ref(), subgraph_error).await {
            Ok(account) => account,
            Err(error) => {
                ctx.add_error(ctx.set_error_path(error.into_server_error(ctx.item.pos)));
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn accounts(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] skip: Option<i32>,
        #[graphql(default = 100)] first: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<AccountOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        #[graphql(name = "where")] filter: Option<AccountEntityFilter>,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Result<Vec<Account>> {
        let state = ctx.data::<AppState>()?;
        let head =
            load_graphql_entity_head(ctx, block.as_ref(), subgraph_error, "accounts").await?;
        let Some((limit, offset)) = generated_page(first, skip) else {
            revalidate_graphql_head(state, head.as_ref(), "accounts").await?;
            return Ok(Vec::new());
        };
        let filter = account_entity_filter_to_storage(filter);
        let chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let rows = load_phase_graphql_account_page_offset(
            &state.pool,
            NAMESPACE,
            &chain_ids,
            &filter,
            generated_order(order_by.map(|_| ()), order_direction),
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("accounts", error))?;
        require_account_rows_at_head(&rows, head.as_ref(), "accounts")?;
        revalidate_graphql_head(state, head.as_ref(), "accounts").await?;
        Ok(rows
            .into_iter()
            .map(|row| Account { id: ID(row.id) })
            .collect())
    }

    /// `domain(id: ID!)` accepts either an ENS name string (for example `"alice.eth"`) or a
    /// namehash. Canonical hash-shaped values resolve by namehash first, then fall back to the name,
    /// so a hash-shaped ENS name cannot shadow an entity ID. Ordinary names take the direct name
    /// lookup path.
    async fn domain(
        &self,
        ctx: &Context<'_>,
        id: ID,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Option<Domain> {
        match resolve_domain(ctx, id, block.as_ref(), subgraph_error).await {
            Ok(domain) => domain,
            Err(error) => {
                ctx.add_error(ctx.set_error_path(error.into_server_error(ctx.item.pos)));
                None
            }
        }
    }

    /// Generated-style offset-paged Domain list.
    #[allow(clippy::too_many_arguments)]
    async fn domains(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] skip: Option<i32>,
        #[graphql(default = 100)] first: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<DomainOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        #[graphql(name = "where")] filter: Option<DomainEntityFilter>,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Result<Vec<Domain>> {
        let (storage_filter, generated_filter) = domain_entity_filter_to_storage(filter)?;
        let state = ctx.data::<AppState>()?;
        let head = load_graphql_entity_head(ctx, block.as_ref(), subgraph_error, "domains").await?;
        let limit = match first {
            Some(first) if first <= 0 => {
                revalidate_graphql_head(state, head.as_ref(), "domains").await?;
                return Ok(Vec::new());
            }
            Some(first) => (first as u64).min(MAX_DOMAINS_PAGE_SIZE),
            None => DEFAULT_DOMAINS_PAGE_SIZE,
        };
        let offset = (skip.unwrap_or(0).max(0) as u64).min(MAX_DOMAINS_SKIP);
        let (sort, order) = generated_domain_sort(order_by, order_direction);
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let rows = load_phase_graphql_name_list_page_offset(
            &state.pool,
            &storage_filter,
            &snapshot_chain_ids,
            &generated_filter,
            sort,
            order,
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("domains", error))?;
        require_rows_at_head(&rows, head.as_ref(), "domains")?;
        revalidate_graphql_head(state, head.as_ref(), "domains").await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let mut domain = Domain::from(row.row);
                domain.served_head = head.clone();
                domain
            })
            .collect())
    }

    async fn resolver(
        &self,
        ctx: &Context<'_>,
        id: ID,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Option<Resolver> {
        match resolve_resolver(ctx, id, block.as_ref(), subgraph_error).await {
            Ok(resolver) => resolver,
            Err(error) => {
                ctx.add_error(ctx.set_error_path(error.into_server_error(ctx.item.pos)));
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolvers(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] skip: Option<i32>,
        #[graphql(default = 100)] first: Option<i32>,
        #[graphql(name = "orderBy")] order_by: Option<ResolverOrderBy>,
        #[graphql(name = "orderDirection")] order_direction: Option<OrderDirection>,
        #[graphql(name = "where")] filter: Option<ResolverEntityFilter>,
        block: Option<BlockHeight>,
        #[graphql(name = "subgraphError", default)] subgraph_error: SubgraphErrorPolicy,
    ) -> Result<Vec<Resolver>> {
        let state = ctx.data::<AppState>()?;
        let head =
            load_graphql_entity_head(ctx, block.as_ref(), subgraph_error, "resolvers").await?;
        let Some((limit, offset)) = generated_page(first, skip) else {
            revalidate_graphql_head(state, head.as_ref(), "resolvers").await?;
            return Ok(Vec::new());
        };
        let Some(filter) = resolver_entity_filter_to_storage(filter) else {
            revalidate_graphql_head(state, head.as_ref(), "resolvers").await?;
            return Ok(Vec::new());
        };
        let chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let rows = load_phase_graphql_resolver_page_offset(
            &state.pool,
            NAMESPACE,
            &chain_ids,
            &filter,
            generated_order(order_by.map(|_| ()), order_direction),
            limit,
            offset,
        )
        .await
        .map_err(|error| internal_error("resolvers", error))?;
        require_resolver_rows_at_head(&rows, head.as_ref(), "resolvers")?;
        hydrate_resolver_rows(ctx, rows, head.as_ref(), "resolvers").await
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
        let head = load_graphql_head(ctx, "registrationConnection").await?;
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let count =
            count_phase_graphql_name_list(&state.pool, &storage_filter, &snapshot_chain_ids)
                .await
                .map_err(|error| internal_error("registrationConnection", error))?;
        require_count_at_head(&count, head.as_ref(), "registrationConnection")?;
        revalidate_graphql_head(state, head.as_ref(), "registrationConnection").await?;
        Ok(RegistrationConnection {
            total_count: Some(count_to_i32(count.total_count)),
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
        let head = load_graphql_head(ctx, "domainConnection").await?;
        let snapshot_chain_ids = graphql_snapshot_chain_ids(head.as_ref());
        let storage_filter = domain_filter_to_storage(filter)?;
        let count =
            count_phase_graphql_name_list(&state.pool, &storage_filter, &snapshot_chain_ids)
                .await
                .map_err(|error| internal_error("domainConnection", error))?;
        require_count_at_head(&count, head.as_ref(), "domainConnection")?;
        revalidate_graphql_head(state, head.as_ref(), "domainConnection").await?;
        Ok(DomainConnection {
            total_count: Some(count_to_i32(count.total_count)),
        })
    }

    /// Access metadata for the same publication used by GraphQL entity reads.
    #[graphql(name = "_meta")]
    async fn meta(
        &self,
        ctx: &Context<'_>,
        block: Option<BlockHeight>,
    ) -> Result<Option<SubgraphMeta>> {
        resolve_meta(ctx, block).await
    }
}

fn generated_page(first: Option<i32>, skip: Option<i32>) -> Option<(u64, u64)> {
    match first {
        Some(first) if first <= 0 => None,
        Some(first) => Some((
            (first as u64).min(MAX_DOMAINS_PAGE_SIZE),
            (skip.unwrap_or(0).max(0) as u64).min(MAX_DOMAINS_SKIP),
        )),
        None => Some((
            DEFAULT_DOMAINS_PAGE_SIZE,
            (skip.unwrap_or(0).max(0) as u64).min(MAX_DOMAINS_SKIP),
        )),
    }
}

async fn resolve_domain(
    ctx: &Context<'_>,
    id: ID,
    block: Option<&BlockHeight>,
    subgraph_error: SubgraphErrorPolicy,
) -> Result<Option<Domain>> {
    let state = ctx.data::<AppState>()?;
    let head = load_graphql_entity_head(ctx, block, subgraph_error, "domain").await?;
    let id = id.as_str();
    let row = if is_canonical_namehash(id) {
        match load_phase_graphql_name_row_by_namehash(&state.pool, NAMESPACE, id)
            .await
            .map_err(|error| internal_error("domain", error))?
        {
            Some(row) => Some(row),
            None => load_phase_graphql_name_row_by_name(&state.pool, NAMESPACE, id)
                .await
                .map_err(|error| internal_error("domain", error))?,
        }
    } else {
        load_phase_graphql_name_row_by_name(&state.pool, NAMESPACE, id)
            .await
            .map_err(|error| internal_error("domain", error))?
    };
    if let Some(row) = row.as_ref() {
        require_rows_at_head(std::slice::from_ref(row), head.as_ref(), "domain")?;
    }
    revalidate_graphql_head(state, head.as_ref(), "domain").await?;
    Ok(row.map(|row| {
        let mut domain = Domain::from(row.row);
        domain.served_head = head;
        domain
    }))
}

fn is_canonical_namehash(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn generated_domain_sort(
    order_by: Option<DomainOrderBy>,
    order_direction: Option<OrderDirection>,
) -> (GeneratedDomainSort, NameCurrentListOrder) {
    let sort = match order_by.unwrap_or(DomainOrderBy::Id) {
        DomainOrderBy::Id => GeneratedDomainSort::Id,
        DomainOrderBy::CreatedAt => GeneratedDomainSort::Storage(NameCurrentListSort::CreatedAt),
        DomainOrderBy::ExpiryDate => GeneratedDomainSort::Storage(NameCurrentListSort::ExpiryDate),
        DomainOrderBy::RegistrationDate => {
            GeneratedDomainSort::Storage(NameCurrentListSort::RegistrationDate)
        }
        DomainOrderBy::Name => GeneratedDomainSort::Storage(NameCurrentListSort::Name),
        DomainOrderBy::Owner => GeneratedDomainSort::Owner,
        DomainOrderBy::OwnerId => GeneratedDomainSort::OwnerId,
        DomainOrderBy::Resolver => GeneratedDomainSort::Resolver,
    };
    let order = match order_direction.unwrap_or(OrderDirection::Asc) {
        OrderDirection::Asc => NameCurrentListOrder::Asc,
        OrderDirection::Desc => NameCurrentListOrder::Desc,
    };
    (sort, order)
}

fn domain_entity_filter_to_storage(
    filter: Option<DomainEntityFilter>,
) -> Result<(NameCurrentListFilter, GeneratedDomainFilter)> {
    let filter = filter.unwrap_or_default();
    let generated_owner = domain_effective_owner_filter(&filter)?;
    let owner = filter.owner.clone();
    let owner_in = filter.owner_in.clone();
    let storage_filter = NameCurrentListFilter {
        namespace: Some(NAMESPACE.to_owned()),
        address: generated_address_membership(
            owner.clone(),
            owner_in.clone(),
            AddressNameRelation::EffectiveController,
        ),
        ..Default::default()
    };
    let generated_filter = GeneratedDomainFilter {
        id: IdFilter {
            eq: nullable_filter_value(filter.id, |id| id.0),
            not: nullable_filter_value(filter.id_not, |id| id.0),
            gt: required_filter_value(filter.id_gt, "id_gt", |id| id.0)?,
            gte: required_filter_value(filter.id_gte, "id_gte", |id| id.0)?,
            lt: required_filter_value(filter.id_lt, "id_lt", |id| id.0)?,
            lte: required_filter_value(filter.id_lte, "id_lte", |id| id.0)?,
            in_values: required_filter_value(filter.id_in, "id_in", |ids| {
                ids.into_iter().map(|id| id.0).collect()
            })?,
            not_in_values: required_filter_value(filter.id_not_in, "id_not_in", |ids| {
                ids.into_iter().map(|id| id.0).collect()
            })?,
        },
        name: StringFilter {
            eq: nullable_filter_value(filter.name, std::convert::identity),
            not: nullable_filter_value(filter.name_not, std::convert::identity),
            gt: required_filter_value(filter.name_gt, "name_gt", std::convert::identity)?,
            gte: required_filter_value(filter.name_gte, "name_gte", std::convert::identity)?,
            lt: required_filter_value(filter.name_lt, "name_lt", std::convert::identity)?,
            lte: required_filter_value(filter.name_lte, "name_lte", std::convert::identity)?,
            in_values: required_filter_value(filter.name_in, "name_in", std::convert::identity)?,
            not_in_values: required_filter_value(
                filter.name_not_in,
                "name_not_in",
                std::convert::identity,
            )?,
            contains: required_filter_value(
                filter.name_contains,
                "name_contains",
                std::convert::identity,
            )?,
            contains_nocase: required_filter_value(
                filter.name_contains_nocase,
                "name_contains_nocase",
                std::convert::identity,
            )?,
            not_contains: required_filter_value(
                filter.name_not_contains,
                "name_not_contains",
                std::convert::identity,
            )?,
            not_contains_nocase: required_filter_value(
                filter.name_not_contains_nocase,
                "name_not_contains_nocase",
                std::convert::identity,
            )?,
            starts_with: required_filter_value(
                filter.name_starts_with,
                "name_starts_with",
                std::convert::identity,
            )?,
            starts_with_nocase: required_filter_value(
                filter.name_starts_with_nocase,
                "name_starts_with_nocase",
                std::convert::identity,
            )?,
            not_starts_with: required_filter_value(
                filter.name_not_starts_with,
                "name_not_starts_with",
                std::convert::identity,
            )?,
            not_starts_with_nocase: required_filter_value(
                filter.name_not_starts_with_nocase,
                "name_not_starts_with_nocase",
                std::convert::identity,
            )?,
            ends_with: required_filter_value(
                filter.name_ends_with,
                "name_ends_with",
                std::convert::identity,
            )?,
            ends_with_nocase: required_filter_value(
                filter.name_ends_with_nocase,
                "name_ends_with_nocase",
                std::convert::identity,
            )?,
            not_ends_with: required_filter_value(
                filter.name_not_ends_with,
                "name_not_ends_with",
                std::convert::identity,
            )?,
            not_ends_with_nocase: required_filter_value(
                filter.name_not_ends_with_nocase,
                "name_not_ends_with_nocase",
                std::convert::identity,
            )?,
        },
        owner: generated_owner,
    };
    Ok((storage_filter, generated_filter))
}

fn nullable_filter_value<T, U>(
    value: MaybeUndefined<T>,
    map: impl FnOnce(T) -> U,
) -> Option<Option<U>> {
    match value {
        MaybeUndefined::Undefined => None,
        MaybeUndefined::Null => Some(None),
        MaybeUndefined::Value(value) => Some(Some(map(value))),
    }
}

fn required_filter_value<T, U>(
    value: MaybeUndefined<T>,
    member: &str,
    map: impl FnOnce(T) -> U,
) -> Result<Option<U>> {
    match value {
        MaybeUndefined::Undefined => Ok(None),
        MaybeUndefined::Null => Err(async_graphql::Error::new(format!(
            "Domain_filter.{member} must not be null"
        ))),
        MaybeUndefined::Value(value) => Ok(Some(map(value))),
    }
}

fn generated_address_membership(
    single: Option<String>,
    many: Option<Vec<String>>,
    relation: AddressNameRelation,
) -> Option<NameCurrentAddressFilter> {
    match (single.map(|value| value.to_lowercase()), many) {
        (Some(single), Some(many)) => {
            let addresses = many
                .into_iter()
                .map(|value| value.to_lowercase())
                .filter(|value| value == &single)
                .collect::<Vec<_>>();
            Some(NameCurrentAddressFilter {
                address: single,
                relation: NameCurrentAddressRelationFilter::Relation(relation),
                addresses: Some(addresses),
            })
        }
        (single, many) => address_membership(single, many, relation),
    }
}

fn domain_filter_to_storage(filter: Option<DomainFilter>) -> Result<NameCurrentListFilter> {
    let filter = filter.unwrap_or_default();
    let contains = filter
        .name_contains
        .as_deref()
        .map(crate::name_filter::normalize_name_contains)
        .transpose()
        .map_err(|error| {
            async_graphql::Error::new(format!(
                "name_contains must be a valid ENSIP-15 name substring: {}",
                error.message()
            ))
        })?;
    Ok(NameCurrentListFilter {
        namespace: Some(NAMESPACE.to_owned()),
        name: filter.name,
        contains,
        address: address_membership(
            filter.owner,
            filter.owner_in,
            AddressNameRelation::TokenHolder,
        ),
        is_migrated: filter.is_migrated,
        ..Default::default()
    })
}

/// Build a storage address-membership filter from a single address and/or an address list, under a
/// fixed relation. A *provided* list takes precedence (subgraph `owner_in`/`registrant_in`) and is
/// honoured exactly — including an empty list, which matches NOTHING (`anc.address = ANY('{}')`),
/// per the compatibility contract. Only a *missing* list (`None`) falls back to the scalar
/// `owner`/`registrant`. Addresses are lowercased to match the stored `address_names_current`
/// convention.
fn address_membership(
    single: Option<String>,
    many: Option<Vec<String>>,
    relation: AddressNameRelation,
) -> Option<NameCurrentAddressFilter> {
    let relation = NameCurrentAddressRelationFilter::Relation(relation);
    match many {
        Some(many) => {
            let many: Vec<String> = many.into_iter().map(|a| a.to_lowercase()).collect();
            Some(NameCurrentAddressFilter {
                // `address` is unused when `addresses` is set (the CTE binds `= ANY($addresses)`);
                // default it for the empty-list case where there is no first element.
                address: many.first().cloned().unwrap_or_default(),
                relation,
                addresses: Some(many),
            })
        }
        None => single.map(|address| NameCurrentAddressFilter {
            address: address.to_lowercase(),
            relation,
            addresses: None,
        }),
    }
}

/// Subgraph `totalCount` is an `Int`; saturate the storage `u64` count into `i32`.
fn count_to_i32(count: u64) -> i32 {
    i32::try_from(count).unwrap_or(i32::MAX)
}
