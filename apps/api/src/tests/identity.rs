#[tokio::test]
async fn identity_forward_single_and_batch_use_partner_not_found_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:alice.eth",
        "Alice.eth",
        "alice.eth",
        "namehash:alice.eth",
        Uuid::from_u128(0x1d0001),
        Uuid::from_u128(0x1d0002),
        Uuid::from_u128(0x1d0003),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        31,
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/identity/names/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("identity forward request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("success"));
    assert_eq!(payload["record"]["name"], json!("Alice.eth"));
    assert_eq!(payload["record"]["namehash"], json!("namehash:alice.eth"));
    assert_eq!(payload["record"]["owner_address"], json!(address));
    assert_eq!(
        payload["record"]["primary_address"],
        json!("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        payload["record"]["coin_type_addresses"]["60"],
        json!("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(payload["record"]["text_records"]["com.twitter"], json!("@alice"));
    assert_eq!(payload["record"]["text_records"]["avatar"], json!("ipfs://avatar"));
    assert_eq!(payload["record"]["resolver_address"], json!(address));
    assert_eq!(payload["record"]["expiration"], json!(1_900_000_000));
    assert_eq!(payload["record"]["network"], json!("ethereum"));
    assert!(payload["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array")
        .contains(&json!("manager_address")));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/identity/names/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("identity forward miss request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload, json!({"status": "not_found", "record": null}));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity/names:batch")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "names": ["missing.eth", "alice.eth", "alice.eth"]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("identity forward batch request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["results"][0]["input"]["name"], json!("missing.eth"));
    assert_eq!(payload["results"][0]["status"], json!("not_found"));
    assert_eq!(payload["results"][0]["record"], Value::Null);
    assert_eq!(payload["results"][1]["input"]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][1]["status"], json!("success"));
    assert_eq!(payload["results"][2]["record"]["name"], json!("Alice.eth"));

    database
        .seed_name_current_binding_migrated(
            "ens:no-records.eth",
            Uuid::from_u128(0x1d0101),
            Uuid::from_u128(0x1d0102),
            Uuid::from_u128(0x1d0103),
        )
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:no-records.eth",
            "no-records.eth",
            "no-records.eth",
            "namehash:no-records.eth",
            Uuid::from_u128(0x1d0103),
            Uuid::from_u128(0x1d0101),
            Some(Uuid::from_u128(0x1d0102)),
            32,
            compact_name_declared_summary(
                address,
                address,
                address,
                1_900_000_000,
                "2026-04-17T00:00:21Z",
                "2026-04-17T00:00:11Z",
            ),
        ))
        .await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/identity/names/no-records.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("identity forward missing record inventory request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    let unsupported_fields = payload["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array");
    assert!(unsupported_fields.contains(&json!("coin_type_addresses")));
    assert!(unsupported_fields.contains(&json!("primary_address")));
    assert!(unsupported_fields.contains(&json!("text_records")));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_batch_routes_map_json_rejections_to_invalid_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for (uri, body) in [
        ("/v1/identity/names:batch", r#"{"names":"not array"}"#),
        ("/v1/identity/addresses:names:batch", r#"{"inputs":"not array"}"#),
        ("/v1/identity/names:batch", r#"{"names":"#),
        ("/v1/identity/addresses:names:batch", r#"{"inputs":"#),
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("identity batch malformed JSON request failed for {uri}"))?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_reverse_marks_primary_orders_and_batches_by_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let managed = "0x0000000000000000000000000000000000000def";
    seed_identity_name(
        &database,
        "ens:alice.eth",
        "Alice.eth",
        "alice.eth",
        "namehash:alice.eth",
        Uuid::from_u128(0x2d0001),
        Uuid::from_u128(0x2d0002),
        Uuid::from_u128(0x2d0003),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        41,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:bob.eth",
        "Bob.eth",
        "bob.eth",
        "namehash:bob.eth",
        Uuid::from_u128(0x2d0011),
        Uuid::from_u128(0x2d0012),
        Uuid::from_u128(0x2d0013),
        address,
        bigname_storage::AddressNameRelation::EffectiveController,
        42,
    )
    .await?;
    bigname_storage::upsert_primary_name_current_snapshots(
        &database.pool,
        &[bigname_storage::PrimaryNameCurrentSnapshot {
            row: bigname_storage::PrimaryNameCurrentRow {
                address: address.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({"source": "identity_test"}),
            },
            normalized_claim_name: Some("alice.eth".to_owned()),
        }],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/identity/addresses/{address}/names?coin_type=60&roles=BOTH&page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("identity reverse request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["input"]["roles"], json!("BOTH"));
    assert_eq!(payload["records"][0]["name"], json!("Alice.eth"));
    assert_eq!(payload["records"][0]["is_primary"], json!(true));
    assert_eq!(payload["records"][0]["relation_facets"], json!(["OWNED"]));
    assert_eq!(payload["pagination"]["has_more"], json!(true));
    let cursor = payload["pagination"]["next_page_cursor"]
        .as_str()
        .expect("first page must include cursor");

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/identity/addresses/{address}/names?coin_type=60&roles=BOTH&page_cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("identity reverse second page request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["records"][0]["name"], json!("Bob.eth"));
    assert_eq!(
        payload["records"][0]["relation_facets"],
        json!(["MANAGED", "EFFECTIVE_CONTROLLER"])
    );
    assert_eq!(payload["pagination"]["has_more"], json!(false));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity/addresses:names:batch")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "inputs": [
                            {"address": address, "coin_type": 60, "roles": "OWNED"},
                            {"address": managed, "coin_type": 60, "roles": "BOTH"}
                        ]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("identity reverse batch request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["results"][0]["input"]["address"], json!(address));
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("Alice.eth"));
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(payload["results"][1]["input"]["address"], json!(managed));
    assert_eq!(
        payload["results"][1]["records"]
            .as_array()
            .expect("records must be array")
            .len(),
        0
    );
    assert_eq!(payload["results"][1]["status"], json!("success"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_reports_projection_lag_by_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES
            ('ethereum-mainnet', '0xstatus09', 9, '2026-04-17T00:00:09Z', 'canonical'),
            ('ethereum-mainnet', '0xstatus10', 10, '2026-04-17T00:00:10Z', 'canonical')
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES (
            'ethereum-mainnet',
            '0xstatus10',
            10,
            '0xstatus09',
            9,
            '0xstatus09',
            9
        )
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state,
            observed_at
        )
        VALUES (
            'status-event-10',
            'ens',
            'status.eth',
            'NameRegistered',
            'ens_v1_registry_l1',
            1,
            'ethereum-mainnet',
            10,
            '0xstatus10',
            '{}'::jsonb,
            'status-test',
            'canonical',
            '{}'::jsonb,
            '{}'::jsonb,
            '2026-04-17T00:00:10Z'
        )
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (
            cursor_name,
            last_change_id,
            updated_at
        )
        SELECT
            'normalized_events_to_projection_invalidations',
            MAX(change_id),
            '2026-04-17T00:00:10Z'
        FROM projection_normalized_event_changes
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            first_change_id,
            last_change_id,
            first_normalized_event_id,
            last_normalized_event_id,
            last_changed_at,
            invalidated_at
        )
        SELECT
            'name_current',
            'status.eth',
            '{}'::jsonb,
            change.change_id,
            change.change_id,
            event.normalized_event_id,
            event.normalized_event_id,
            change.changed_at,
            change.changed_at
        FROM projection_normalized_event_changes change
        JOIN normalized_events event
          ON event.normalized_event_id = change.normalized_event_id
        WHERE event.event_identity = 'status-event-10'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status/indexing")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("stale"));
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["canonical_block"],
        json!(10)
    );
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(9)
    );
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["projection_lag_blocks"],
        json!(1)
    );
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["projection_lag_seconds"],
        json!(1)
    );

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(&database.pool)
        .await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status/indexing")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ready indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["status"], json!("ready"));
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(10)
    );
    assert_eq!(
        payload["chains"]["ethereum-mainnet"]["projection_lag_blocks"],
        json!(0)
    );

    database.cleanup().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_identity_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    normalized_name: &str,
    namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    block_number: i64,
) -> Result<()> {
    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            logical_name_id,
            display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            block_number,
            compact_name_declared_summary(
                address,
                address,
                address,
                1_900_000_000,
                "2026-04-17T00:00:21Z",
                "2026-04-17T00:00:11Z",
            ),
        ))
        .await?;
    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            logical_name_id,
            relation,
            display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            block_number,
        )],
    )
    .await?;

    Ok(())
}
