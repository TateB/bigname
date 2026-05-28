use anyhow::{Context, Result};
use axum::{Json, response::Html, routing::get};
use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;
use tracing::info;

use crate::{
    API_ROUTE_DEFINITIONS, AppState, Router, ServeArgs, shutdown_signal,
    warm_compact_records_route_sql_path,
};

use super::schemas::openapi_components;

const OPENAPI_DOCS_HTML: &str = include_str!("docs.html");

pub(crate) async fn serve(args: ServeArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let chain_rpc_urls = args.effective_chain_rpc_urls()?;
    let state = AppState {
        phase: bigname_domain::bootstrap_phase(),
        pool,
        chain_rpc_urls,
    };
    warm_compact_records_route_sql_path(&state, args.database.max_connections)
        .await
        .context("failed to warm compact records route SQL path")?;
    let router = app_router(state);
    let listener = tokio::net::TcpListener::bind(args.bind_addr)
        .await
        .context("failed to bind the API listener")?;

    info!(
        service = "api",
        bind_addr = %args.bind_addr,
        phase = bigname_domain::bootstrap_phase(),
        "API booted"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal("api"))
        .await
        .context("API server exited unexpectedly")
}

pub(crate) fn app_router(state: AppState) -> Router {
    API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .fold(Router::new(), |router, route| route.register(router))
        .route("/", get(openapi_docs))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(openapi_docs))
        .route("/docs/", get(openapi_docs))
        .with_state(state)
}

async fn openapi_json() -> Json<JsonValue> {
    Json(openapi_document())
}

async fn openapi_docs() -> Html<&'static str> {
    Html(OPENAPI_DOCS_HTML)
}

pub(crate) fn render_openapi_document() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&openapi_document()).expect("OpenAPI document must render");
    rendered.push('\n');
    rendered
}

pub(crate) fn openapi_document() -> JsonValue {
    let mut paths = JsonMap::new();
    for route in API_ROUTE_DEFINITIONS {
        if let Some(path_item) = route.openapi_path_item() {
            paths.insert(route.path.to_owned(), path_item);
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "bigname API v1",
            "version": "phase-7",
            "description": "Machine-readable publication of the currently shipped public API surface derived from apps/api/src/main.rs.",
        },
        "paths": JsonValue::Object(paths),
        "components": openapi_components(),
    })
}
