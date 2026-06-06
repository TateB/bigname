use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use axum::{Router, routing::post};

use crate::state::AppState;

use super::http::{graphiql, graphql_handler};
use super::query::QueryRoot;

pub(crate) type SubgraphSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

fn build_schema(state: AppState) -> SubgraphSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(state)
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
