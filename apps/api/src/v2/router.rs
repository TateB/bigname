use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};

use crate::state::AppState;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v2/lookup", post(not_implemented))
        .route("/v2/status", get(not_implemented))
        .route("/v2/names/{name}", get(not_implemented))
        .route("/v2/names/{name}/records", get(not_implemented))
        .route("/v2/names/{name}/subnames", get(not_implemented))
        .route("/v2/names/{name}/history", get(not_implemented))
        .route("/v2/permissions", get(not_implemented))
        .route("/v2/addresses/{address}/names", get(not_implemented))
        .route("/v2/addresses/{address}/primary-name", get(not_implemented))
        .route("/v2/addresses/{address}/history", get(not_implemented))
        .route("/v2/search", get(not_implemented))
        .route("/v2/events", get(not_implemented))
        .route("/v2/resolvers/{chain_id}/{address}", get(not_implemented))
        .route("/v2/namespaces/{namespace}", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/coverage",
            get(not_implemented),
        )
        .route("/v2/diagnostics/names/{name}/binding", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/authority",
            get(not_implemented),
        )
        .route("/v2/diagnostics/names/{name}/records", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/execution",
            get(not_implemented),
        )
        .route(
            "/v2/diagnostics/namespaces/{namespace}/manifests",
            get(not_implemented),
        )
        .route("/v2/diagnostics/events", get(not_implemented))
}

async fn not_implemented() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}
