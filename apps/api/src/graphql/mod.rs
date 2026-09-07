//! Native subgraph-compatible GraphQL surface for the documented compatibility subset.
//!
//! Serves generated Account, Domain, and Resolver entity roots plus the existing connection roots
//! over the current schema-v2 projections, preserving the subgraph field
//! shapes exercised by the committed GraphQL schema fixture and API tests.
//! Resolver record fields (`texts`/`contentHash`/`addresses`) are served from the name's
//! `record_inventory_current` projection (text selector keys, retained addr/contenthash values).

mod account_queries;
mod convert;
mod effective_owner_filter;
mod enums;
mod error;
mod generated_filter_ops;
mod http;
mod inputs;
mod loader;
mod meta;
mod name_queries;
mod objects;
mod query;
mod record_inventory_query;
mod resolver_queries;
mod scalars;
mod schema;
mod snapshot;

#[cfg(test)]
pub(crate) use account_queries::{GeneratedAccountFilter, explain_phase_graphql_account_page};
#[cfg(test)]
pub(crate) use generated_filter_ops::{GeneratedDomainFilter, push_generated_domain_filters};
#[cfg(test)]
pub(crate) use name_queries::{
    GeneratedDomainSort, count_phase_graphql_name_list, explain_phase_graphql_name_list_page,
    load_phase_graphql_name_list_page_offset, push_filtered_names,
};
#[cfg(test)]
pub(crate) use resolver_queries::{
    GeneratedResolverFilter, explain_phase_graphql_resolver_page,
    load_phase_graphql_resolver_page_offset, parse_resolver_id,
};
pub(crate) use schema::graphql_routes;
#[cfg(test)]
pub(crate) use schema::subgraph_sdl;
#[cfg(test)]
pub(crate) use snapshot::graphql_indexing_status_test_hooks;
#[cfg(test)]
pub(crate) use snapshot::nested_inventory_test_hooks;
