//! Native subgraph-compatible GraphQL surface for the ENS Manager dashboard.
//!
//! Serves the minimal four-operation subset (`domain`, `domains`, `registrationConnection`,
//! `domainConnection`) over the existing `bigname_storage` reads, preserving the subgraph field
//! shapes the Manager's committed codegen expects, so the Manager can point at bigname unchanged.
//! Resolver record fields (`texts`/`contentHash`/`addresses`) are served from the name's
//! `record_inventory_current` projection (text selector keys, retained addr/contenthash values).

mod convert;
mod enums;
mod error;
mod http;
mod inputs;
mod loader;
mod objects;
mod query;
mod schema;

pub(crate) use schema::graphql_routes;
#[cfg(test)]
pub(crate) use schema::subgraph_sdl;
