#[derive(Clone, Copy)]
struct V2StrictQueryCase {
    method: V2StrictQueryMethod,
    uri: &'static str,
    expected_message: &'static str,
}

#[derive(Clone, Copy)]
enum V2StrictQueryMethod {
    Get,
    PostLookup,
}

#[tokio::test]
async fn v2_routes_reject_undocumented_query_params_family_wide() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    // Strict query extractors run before handler resource resolution, so the
    // bogus-param conformance check does not need per-route seed rows.
    for case in V2_STRICT_QUERY_CASES {
        let response = v2_strict_query_response(&database, case).await?;
        assert_v2_invalid_input(response, case.uri, case.expected_message).await?;
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_routes_reject_known_but_inapplicable_query_params() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for (uri, expected_message) in [
        (
            "/v2/events?dedupe=name",
            "unknown query parameter: dedupe",
        ),
        ("/v2/names/alice.eth?q=x", "unknown query parameter: q"),
        (
            "/v2/addresses/0x00000000000000000000000000000000000000aa/names?match=contains",
            "unknown query parameter: match",
        ),
        (
            "/v2/diagnostics/events?relation=registrant",
            "unknown query parameter: relation",
        ),
        (
            "/v2/resolvers/1/0x00000000000000000000000000000000000000aa?namespace=ens",
            "unknown query parameter: namespace",
        ),
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("v2 inapplicable query request failed for {uri}"))?;

        assert_v2_invalid_input(response, uri, expected_message).await?;
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_documented_query_params_remain_accepted_for_positive_controls() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let status_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 status positive-control request failed")?;
    assert_eq!(status_response.status(), StatusCode::OK);
    database.cleanup().await?;

    let name_payload = v2_name_record_payload("/v2/names/Alice.eth?source=verified").await?;
    assert_eq!(name_payload["meta"]["source"], json!("verified"));

    Ok(())
}

const V2_STRICT_QUERY_CASES: &[V2StrictQueryCase] = &[
    V2StrictQueryCase {
        method: V2StrictQueryMethod::PostLookup,
        uri: "/v2/lookup",
        expected_message: "query parameters are not supported on this route",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/status",
        expected_message: "query parameters are not supported on this route",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/names/alice.eth",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/names/alice.eth/records",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/names/alice.eth/subnames",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/names/alice.eth/history",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/permissions",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/names",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/primary-name",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/history",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/search",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/events",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/resolvers/1/0x00000000000000000000000000000000000000aa",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/namespaces/ens",
        expected_message: "query parameters are not supported on this route",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/names/alice.eth/coverage",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/names/alice.eth/binding",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/names/alice.eth/authority",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/names/alice.eth/records",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/names/alice.eth/execution",
        expected_message: "unknown query parameter: bogus_param",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/namespaces/ens/manifests",
        expected_message: "query parameters are not supported on this route",
    },
    V2StrictQueryCase {
        method: V2StrictQueryMethod::Get,
        uri: "/v2/diagnostics/events",
        expected_message: "unknown query parameter: bogus_param",
    },
];

async fn v2_strict_query_response(
    database: &TestDatabase,
    case: &V2StrictQueryCase,
) -> Result<Response> {
    let uri = format!("{}?bogus_param=1", case.uri);
    let mut request = Request::builder().uri(uri.as_str());
    let body = match case.method {
        V2StrictQueryMethod::Get => Body::empty(),
        V2StrictQueryMethod::PostLookup => {
            request = request.method("POST").header("content-type", "application/json");
            Body::from(r#"{"inputs":[{"id":"name","name":"alice.eth"}]}"#)
        }
    };

    app_router(database.app_state())
        .oneshot(request.body(body).expect("request must build"))
        .await
        .with_context(|| format!("v2 strict query request failed for {uri}"))
}

async fn assert_v2_invalid_input(
    response: Response,
    uri: &str,
    expected_message: &str,
) -> Result<()> {
    assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload,
        json!({
            "error": {
                "code": "invalid_input",
                "message": expected_message,
                "details": {}
            }
        }),
        "{uri}"
    );
    Ok(())
}
