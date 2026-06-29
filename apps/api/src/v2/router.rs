use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};

use crate::AppState;

use super::{
    get_address_history, get_address_names, get_events, get_history, get_name_record,
    get_name_records, get_namespace, get_permissions, get_primary_name, get_resolver, get_status,
    get_subnames,
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v2/lookup", post(not_implemented))
        .route("/v2/status", get(get_status))
        .route("/v2/names/{name}", get(get_name_record))
        .route("/v2/names/{name}/records", get(get_name_records))
        .route("/v2/names/{name}/subnames", get(get_subnames))
        .route("/v2/names/{name}/history", get(get_history))
        .route("/v2/permissions", get(get_permissions))
        .route("/v2/addresses/{address}/names", get(get_address_names))
        .route(
            "/v2/addresses/{address}/primary-name",
            get(get_primary_name),
        )
        .route("/v2/addresses/{address}/history", get(get_address_history))
        .route("/v2/search", get(not_implemented))
        .route("/v2/events", get(get_events))
        .route("/v2/resolvers/{chain_id}/{address}", get(get_resolver))
        .route("/v2/namespaces/{namespace}", get(get_namespace))
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

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use tower::ServiceExt;

    use crate::AppState;

    use super::*;

    #[tokio::test]
    async fn status_route_rejects_query_params_with_v2_error_envelope() {
        let state = AppState {
            phase: "test",
            pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
                .expect("query rejection does not use the database"),
            chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
        };

        let response = router()
            .with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/v2/status?at=2026-06-10T00:00:00Z")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .expect("status route request must complete");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body must read");
        let payload: Value = serde_json::from_slice(&body).expect("response must be JSON");

        assert_eq!(
            payload,
            json!({
                "error": {
                    "code": "invalid_input",
                    "message": "query parameters are not supported on this route",
                    "details": {}
                }
            })
        );
    }
}
