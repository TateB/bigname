#[tokio::test]
async fn get_events_returns_compact_canonical_rows_with_projection_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xe001);
    let surface_binding_id = Uuid::from_u128(0xe101);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            logical_name_id,
            bigname_storage::AddressNameRelation::Registrant,
            "alice.eth",
            "alice.eth",
            "namehash:alice.eth",
            surface_binding_id,
            resource_id,
            None,
            600,
        )],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x600", None, 600, 1_700_000_600),
            raw_block(
                "ethereum-mainnet",
                "0x601",
                Some("0x600"),
                601,
                1_700_000_601,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x602",
                Some("0x601"),
                602,
                1_700_000_602,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                event_kind: "RecordChanged".to_owned(),
                after_state: json!({
                    "record_key": "text:avatar",
                    "value": "ipfs://avatar",
                    "provenance": {
                        "internal": true,
                    },
                    "coverage": {
                        "status": "full",
                    },
                }),
                ..history_event(
                    "events:record",
                    Some(logical_name_id),
                    None,
                    Some("ethereum-mainnet"),
                    Some(600),
                    Some("0x600"),
                    Some("0xtx600"),
                    Some(4),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                event_kind: "RegistrationGranted".to_owned(),
                after_state: json!({
                    "registrant": address.to_ascii_uppercase(),
                }),
                ..history_event(
                    "events:registration",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(601),
                    Some("0x601"),
                    Some("0xtx601"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                event_kind: "RegistrationGranted".to_owned(),
                after_state: json!({
                    "registrant": address,
                }),
                ..history_event(
                    "events:observed",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(602),
                    Some("0x602"),
                    Some("0xtx602"),
                    Some(0),
                    CanonicalityState::Observed,
                )
            },
        ],
    )
    .await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/events?namespace=ens&name=alice.eth&page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("events first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: Value = read_json(first_page_response).await?;
    assert_eq!(
        first_page_payload["data"][0]["type"],
        json!("registration")
    );
    assert_eq!(
        first_page_payload["data"][0]["transaction_hash"],
        json!("0xtx601")
    );
    assert!(first_page_payload["data"][0].get("tx_hash").is_none());
    assert!(first_page_payload.get("provenance").is_none());
    assert!(first_page_payload.get("coverage").is_none());
    assert_eq!(
        first_page_payload["meta"]["support_status"],
        json!("supported")
    );
    let cursor = first_page_payload["page"]["next_cursor"]
        .as_str()
        .expect("events first page must include a next cursor")
        .to_owned();

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/events?namespace=ens&name=alice.eth&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("events second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: Value = read_json(second_page_response).await?;
    assert_eq!(second_page_payload["data"][0]["type"], json!("record"));
    assert_eq!(second_page_payload["data"][0]["name"], json!("alice.eth"));
    assert_eq!(second_page_payload["data"][0]["block_number"], json!(600));
    assert_eq!(
        second_page_payload["data"][0]["timestamp"],
        json!("2023-11-14T22:23:20Z")
    );
    assert_eq!(second_page_payload["data"][0]["log_index"], json!(4));
    assert_eq!(
        second_page_payload["data"][0]["data"],
        json!({
            "record_key": "text:avatar",
            "value": "ipfs://avatar",
        })
    );
    assert!(second_page_payload["data"][0].get("normalized_event_id").is_none());
    assert!(second_page_payload["data"][0].get("raw_fact_ref").is_none());
    assert!(second_page_payload["data"][0].get("provenance").is_none());
    assert!(second_page_payload["data"][0].get("coverage").is_none());

    let spaced_name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/events?namespace=ens&name=%20alice.eth%20")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("events whitespace-padded name filter request failed")?;
    assert_eq!(spaced_name_response.status(), StatusCode::OK);
    let spaced_name_payload: Value = read_json(spaced_name_response).await?;
    assert_eq!(
        spaced_name_payload["data"].as_array().map(Vec::len),
        Some(0)
    );

    let address_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/events?address={address}&relation=registrant&namespace=ens&type=registration&from_block=601&to_block=601"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address-filtered events request failed")?;
    assert_eq!(address_response.status(), StatusCode::OK);
    let address_payload: Value = read_json(address_response).await?;
    assert_eq!(address_payload["data"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        address_payload["data"][0]["transaction_hash"],
        json!("0xtx601")
    );
    assert!(address_payload["data"][0].get("tx_hash").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_events_returns_explicit_errors_for_reserved_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for uri in [
        "/v1/events?name=alice.eth",
        "/v1/events?resource=00000000-0000-0000-0000-000000000001&resource_id=00000000-0000-0000-0000-000000000002",
        "/v1/events?from_block=20&to_block=10",
        "/v1/events?view=full",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("invalid events request failed for {uri}"))?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "invalid_input");
    }

    for uri in [
        "/v1/events?resource_hex=0x1234",
        "/v1/events?selector=text:avatar",
        "/v1/events?type=unknown_alias",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("unsupported events request failed for {uri}"))?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "unsupported");
    }

    database.cleanup().await?;
    Ok(())
}
