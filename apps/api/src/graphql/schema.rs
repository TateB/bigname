use async_graphql::dataloader::DataLoader;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use axum::{Router, routing::post};

use crate::state::AppState;

use super::http::{graphiql, graphql_handler};
use super::loader::RecordInventoryLoader;
use super::query::QueryRoot;

pub(crate) type SubgraphSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Depth/complexity ceilings. The deepest legitimate documents are GraphiQL's introspection query
/// (~14 levels through the nested `ofType` fragment) and the Manager's `Domain` fragment (~5);
/// complexity is counted per field selection, so the full introspection document lands in the low
/// hundreds. The ceilings block pathological nesting / alias-spam without touching real callers.
const MAX_QUERY_DEPTH: usize = 32;
const MAX_QUERY_COMPLEXITY: usize = 4_000;

fn build_schema(state: AppState) -> SubgraphSchema {
    // Batches the per-domain `record_inventory_current` reads behind `Domain.resolver` (the list
    // N+1). `DataLoader::new` uses `NoCache`, so the loader only coalesces a request's reads into
    // one query and never memoizes rows across requests (the projection changes under the indexer).
    let record_inventory_loader =
        DataLoader::new(RecordInventoryLoader::new(state.pool.clone()), tokio::spawn);
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .limit_depth(MAX_QUERY_DEPTH)
        .limit_complexity(MAX_QUERY_COMPLEXITY)
        .data(state)
        .data(record_inventory_loader)
        .finish()
}

/// Build the `/graphql` router carrying the schema as its own router state, so it merges with the
/// REST router as `Router<()>` + `Router<()>` without adding the schema to `AppState`.
pub(crate) fn graphql_routes(state: AppState) -> Router {
    Router::new()
        .route("/graphql", post(graphql_handler).get(graphiql))
        .with_state(build_schema(state))
}

/// Render the schema's SDL (no `AppState` data needed — data does not affect the SDL). Used by the
/// snapshot test that guards the codegen contract.
#[cfg(test)]
pub(crate) fn subgraph_sdl() -> String {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .finish()
        .sdl()
}
