#[tokio::test]
async fn identity_lookup_names_return_native_detail_shape() -> Result<()> {
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

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "missing", "kind": "name", "name": "missing.eth"},
                {"id": "alice-1", "kind": "name", "name": "alice.eth"},
                {"id": "alice-2", "kind": "name", "name": "alice.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["status"], json!("not_found"));
    assert_eq!(payload["results"][0]["record"], Value::Null);
    assert_eq!(payload["results"][1]["input"]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][1]["status"], json!("success"));
    assert_eq!(payload["results"][1]["record"]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][1]["record"]["namespace"], json!("ens"));
    assert_eq!(
        payload["results"][1]["record"]["namehash"],
        json!("namehash:alice.eth")
    );
    assert_eq!(payload["results"][1]["record"]["owner_address"], json!(address));
    assert_eq!(
        payload["results"][1]["record"]["primary_address"],
        json!("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        payload["results"][1]["record"]["coin_type_addresses"]["60"],
        json!("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        payload["results"][1]["record"]["text_records"]["com.twitter"],
        json!("@alice")
    );
    assert_eq!(
        payload["results"][1]["record"]["text_records"]["avatar"],
        json!("ipfs://avatar")
    );
    assert_eq!(payload["results"][1]["record"]["resolver_address"], json!(address));
    assert_eq!(
        payload["results"][1]["record"]["expiration"],
        json!(1_900_000_000)
    );
    assert_eq!(payload["results"][1]["record"]["network"], json!("ethereum"));
    assert!(payload["results"][1]["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array")
        .contains(&json!("manager_address")));
    assert_eq!(payload["results"][2]["record"]["name"], json!("alice.eth"));

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
    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "no-records", "kind": "name", "name": "no-records.eth"}
            ]
        }),
    )
    .await?;
    let unsupported_fields = payload["results"][0]["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array");
    assert!(unsupported_fields.contains(&json!("coin_type_addresses")));
    assert!(unsupported_fields.contains(&json!("primary_address")));
    assert!(unsupported_fields.contains(&json!("text_records")));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_forward_matches_record_inventory_to_active_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:boundary.eth";
    let resource_id = Uuid::from_u128(0x1d0201);
    let token_lineage_id = Uuid::from_u128(0x1d0202);
    let surface_binding_id = Uuid::from_u128(0x1d0203);
    let stale_address = "0x0000000000000000000000000000000000000abc";
    let active_address = "0x0000000000000000000000000000000000000def";
    let active_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    let mut declared_summary = compact_name_declared_summary(
        stale_address,
        stale_address,
        stale_address,
        1_900_000_000,
        "2026-04-17T00:00:21Z",
        "2026-04-17T00:00:11Z",
    );
    declared_summary["topology"] = json!({
        "version_boundaries": {
            "topology_version_boundary": active_boundary.clone(),
            "record_version_boundary": active_boundary.clone(),
        }
    });
    database
        .insert_name_current_row(address_name_name_current_row(
            logical_name_id,
            "Boundary.eth",
            "boundary.eth",
            "namehash:boundary.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            33,
            declared_summary,
        ))
        .await?;

    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;
    let mut active_inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    active_inventory.record_version_boundary = active_boundary;
    active_inventory.entries = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "status": "not_found",
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": active_address,
            },
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://active-avatar",
            },
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://active-content",
            },
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "@boundary-active",
            },
        },
    ]);
    database
        .insert_record_inventory_current_row(active_inventory)
        .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "boundary", "kind": "name", "name": "boundary.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(
        payload["results"][0]["record"]["coin_type_addresses"]["60"],
        json!(active_address)
    );
    assert_eq!(
        payload["results"][0]["record"]["primary_address"],
        json!(active_address)
    );
    assert_eq!(
        payload["results"][0]["record"]["text_records"]["com.twitter"],
        json!("@boundary-active")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_forward_normalizes_inferred_name_inputs() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:case.eth",
        "case.eth",
        "case.eth",
        "namehash:case.eth",
        Uuid::from_u128(0x1d0301),
        Uuid::from_u128(0x1d0302),
        Uuid::from_u128(0x1d0303),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        35,
    )
    .await?;
    seed_identity_name(
        &database,
        "basenames:someone.base.eth",
        "someone.base.eth",
        "someone.base.eth",
        "namehash:someone.base.eth",
        Uuid::from_u128(0x1d0311),
        Uuid::from_u128(0x1d0312),
        Uuid::from_u128(0x1d0313),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        36,
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "case", "kind": "name", "name": "Case.eth"},
                {"id": "bad", "kind": "name", "name": "bad name.eth"},
                {"id": "base", "kind": "name", "name": "Someone.Base.eth"},
                {"id": "trim", "kind": "name", "name": "case.eth "}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(payload["results"][0]["record"]["name"], json!("case.eth"));
    assert_eq!(
        payload["results"][0]["normalization"],
        json!({
            "changed": true,
            "input_name": "Case.eth",
            "reason": "case_normalized",
        })
    );
    assert_eq!(payload["results"][1]["status"], json!("unnormalizable_input"));
    assert_eq!(payload["results"][1]["record"], Value::Null);
    assert_eq!(
        payload["results"][1]["normalization"]["reason"],
        json!("invalid_normalized_name")
    );
    assert_eq!(
        payload["results"][2]["record"]["name"],
        json!("someone.base.eth")
    );
    assert_eq!(payload["results"][2]["record"]["network"], json!("base"));
    assert_eq!(
        payload["results"][3]["normalization"],
        json!({
            "changed": false,
            "input_name": "case.eth ",
            "reason": "invalid_normalized_name",
        })
    );
    assert_eq!(payload["results"][3]["status"], json!("unnormalizable_input"));
    assert_eq!(payload["results"][3]["record"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_lookup_returns_native_slim_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:case.eth",
        "Case.eth",
        "case.eth",
        "namehash:case.eth",
        Uuid::from_u128(0x1d0351),
        Uuid::from_u128(0x1d0352),
        Uuid::from_u128(0x1d0353),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity:lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "profile": "feed",
                        "namespace": "public",
                        "inputs": [
                            {"id": "name-1", "kind": "name", "name": "Case.eth"},
                            {"id": "name-2", "kind": "name", "name": "bad name.eth"},
                            {"id": "name-3", "kind": "name", "name": "case.eth "},
                            {
                                "id": "addr-1",
                                "kind": "address",
                                "address": address,
                                "coin_type": 60,
                                "roles": ["owned", "managed"],
                                "page_size": 1
                            }
                        ]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("native identity lookup request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    assert_eq!(payload["results"][0]["id"], json!("name-1"));
    assert_eq!(payload["results"][0]["kind"], json!("name"));
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(payload["results"][0]["input"], json!({"name": "Case.eth"}));
    assert_eq!(
        payload["results"][0]["normalization"],
        json!({
            "changed": true,
            "input_name": "Case.eth",
            "reason": "case_normalized",
        })
    );
    assert_eq!(payload["results"][0]["record"]["name"], json!("case.eth"));
    assert_eq!(payload["results"][0]["record"]["namespace"], json!("ens"));
    let name_record = payload["results"][0]["record"]
        .as_object()
        .expect("native name record must be an object");
    assert!(!name_record.contains_key("normalized_name"));
    assert!(!name_record.contains_key("corrected_input_normalization"));
    assert!(!name_record.contains_key("as_of"));
    assert!(!name_record.contains_key("owner_address"));
    assert!(!name_record.contains_key("unsupported_fields"));

    assert_eq!(payload["results"][1]["status"], json!("unnormalizable_input"));
    assert_eq!(payload["results"][1]["record"], Value::Null);
    assert_eq!(
        payload["results"][1]["normalization"],
        json!({
            "changed": false,
            "input_name": "bad name.eth",
            "reason": "invalid_normalized_name",
        })
    );
    assert_eq!(payload["results"][2]["status"], json!("unnormalizable_input"));
    assert_eq!(payload["results"][2]["record"], Value::Null);
    assert_eq!(
        payload["results"][2]["normalization"],
        json!({
            "changed": false,
            "input_name": "case.eth ",
            "reason": "invalid_normalized_name",
        })
    );

    assert_eq!(payload["results"][3]["kind"], json!("address"));
    assert_eq!(payload["results"][3]["status"], json!("success"));
    assert_eq!(
        payload["results"][3]["input"],
        json!({
            "address": address,
            "coin_type": 60,
            "roles": ["owned", "managed"],
        })
    );
    assert_eq!(payload["results"][3]["records"][0]["name"], json!("case.eth"));
    assert_eq!(
        payload["results"][3]["records"][0]["relation_facets"],
        json!(["owned"])
    );
    assert_eq!(payload["results"][3]["page"]["total_count"], json!(1));
    assert_eq!(payload["results"][3]["page"]["next_cursor"], Value::Null);
    let feed_record = payload["results"][3]["records"][0]
        .as_object()
        .expect("native feed record must be an object");
    assert!(!feed_record.contains_key("normalized_name"));
    assert!(!feed_record.contains_key("owner_address"));
    assert!(!feed_record.contains_key("unsupported_fields"));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity:lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "profile": "feed",
                        "namespace": "ens",
                        "inputs": [
                            {"id": "name-1", "kind": "name", "name": "Case.eth"}
                        ]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("native identity lookup explicit namespace name request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["results"][0]["record"]["namespace"], json!("ens"));
    assert_eq!(payload["results"][0]["record"]["name"], json!("case.eth"));

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity:lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "profile": "feed",
                        "namespace": "ens",
                        "inputs": [
                            {
                                "id": "addr-1",
                                "kind": "address",
                                "address": address,
                                "coin_type": 60
                            }
                        ]
                    }))
                    .expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("native identity lookup explicit namespace address request failed")?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert!(
        payload["error"]["message"]
            .as_str()
            .expect("error message must be a string")
            .contains("explicit namespace filters")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_network_uses_runtime_chain_positions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:sepolia.eth",
        "sepolia.eth",
        "sepolia.eth",
        "namehash:sepolia.eth",
        Uuid::from_u128(0x1d0401),
        Uuid::from_u128(0x1d0402),
        Uuid::from_u128(0x1d0403),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        37,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE name_current
        SET chain_positions = $2::JSONB
        WHERE logical_name_id = $1
        "#,
    )
    .bind("ens:sepolia.eth")
    .bind(
        serde_json::to_string(&json!({
            "ethereum": {
                "chain_id": "ethereum-sepolia",
                "block_number": 37,
                "block_hash": "0xsepolia37",
                "timestamp": "2026-04-17T00:00:37Z"
            }
        }))
        .expect("chain positions JSON must serialize"),
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE address_names_current
        SET chain_positions = $2::JSONB,
            coverage = $3::JSONB
        WHERE logical_name_id = $1
        "#,
    )
    .bind("ens:sepolia.eth")
    .bind(
        serde_json::to_string(&json!({
            "ethereum": {
                "chain_id": "ethereum-sepolia",
                "block_number": 37,
                "block_hash": "0xsepolia37",
                "timestamp": "2026-04-17T00:00:37Z"
            }
        }))
        .expect("chain positions JSON must serialize"),
    )
    .bind(
        serde_json::to_string(&json!({
            "status": "stale"
        }))
        .expect("coverage JSON must serialize"),
    )
    .execute(&database.pool)
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "name", "kind": "name", "name": "sepolia.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["record"]["network"],
        json!("ethereum-sepolia")
    );

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [
                {
                    "id": "address",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"]
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"][0]["network"],
        json!("ethereum-sepolia")
    );
    assert_eq!(payload["results"][0]["records"][0]["status"], json!("stale"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_forward_labelhash_token_id_fallback_stays_ens_erc721_only() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let eth_labelhash = "0x00000000000000000000000000000000000000000000000000000000000000ee";

    seed_identity_name(
        &database,
        "ens:fallback.eth",
        "fallback.eth",
        "fallback.eth",
        "namehash:fallback.eth",
        Uuid::from_u128(0x1d0201),
        Uuid::from_u128(0x1d0202),
        Uuid::from_u128(0x1d0203),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        33,
    )
    .await?;
    set_name_surface_labelhashes(
        &database,
        "ens:fallback.eth",
        &[
            "0x000000000000000000000000000000000000000000000000000000000000000a",
            eth_labelhash,
        ],
    )
    .await?;

    seed_identity_name(
        &database,
        "ens:child.parent.eth",
        "child.parent.eth",
        "child.parent.eth",
        "namehash:child.parent.eth",
        Uuid::from_u128(0x1d0211),
        Uuid::from_u128(0x1d0212),
        Uuid::from_u128(0x1d0213),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        34,
    )
    .await?;
    set_name_surface_labelhashes(
        &database,
        "ens:child.parent.eth",
        &[
            "0x000000000000000000000000000000000000000000000000000000000000000b",
            "0x000000000000000000000000000000000000000000000000000000000000000c",
            eth_labelhash,
        ],
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "fallback", "kind": "name", "name": "fallback.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["record"]["token_id"], json!("10"));
    assert!(!payload["results"][0]["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array")
        .contains(&json!("token_id")));

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {"id": "child", "kind": "name", "name": "child.parent.eth"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["record"]["token_id"], Value::Null);
    assert!(payload["results"][0]["record"]["unsupported_fields"]
        .as_array()
        .expect("unsupported_fields must be array")
        .contains(&json!("token_id")));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_batch_routes_map_json_rejections_to_invalid_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for body in [
        r#"{"inputs":"not array"}"#,
        r#"{"inputs":"#,
        r#"{"profile":"other","inputs":[]}"#,
        r#"{"inputs":[{"id":"addr","kind":"address","address":"0x0000000000000000000000000000000000000abc"}]}"#,
        r#"{"profile":"feed","namespace":"ens","inputs":[{"id":"addr","kind":"address","address":"0x0000000000000000000000000000000000000abc","coin_type":60}]}"#,
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/identity:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request must build"),
            )
            .await
            .context("native identity lookup malformed JSON request failed")?;
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
        &[
            bigname_storage::PrimaryNameCurrentSnapshot {
                row: bigname_storage::PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                    raw_claim_name: None,
                    claim_provenance: json!({"source": "identity_test"}),
                },
                normalized_claim_name: Some("alice.eth".to_owned()),
            },
            bigname_storage::PrimaryNameCurrentSnapshot {
                row: bigname_storage::PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "8453".to_owned(),
                    claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                    raw_claim_name: None,
                    claim_provenance: json!({"source": "identity_test"}),
                },
                normalized_claim_name: Some("bob.eth".to_owned()),
            },
        ],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE record_inventory_current
        SET
            selectors = selectors || $2::JSONB,
            entries = entries || $3::JSONB
        WHERE resource_id = $1
        "#,
    )
    .bind(Uuid::from_u128(0x2d0011))
    .bind(
        serde_json::to_string(&json!([
            {
                "record_key": "addr:8453",
                "record_family": "addr",
                "selector_key": "8453",
                "cacheable": true,
            }
        ]))
        .expect("selector JSON must serialize"),
    )
    .bind(
        serde_json::to_string(&json!([
            {
                "record_key": "addr:8453",
                "record_family": "addr",
                "selector_key": "8453",
                "status": "success",
                "value": {
                    "coin_type": "8453",
                    "value": "0x0000000000000000000000000000000000000def",
                },
            }
        ]))
        .expect("entry JSON must serialize"),
    )
    .execute(&database.pool)
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "page_size": 1
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["input"]["roles"],
        json!(["owned", "managed"])
    );
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["owned"])
    );
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(true));
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));
    let cursor = payload["results"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include cursor");

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "cursor": cursor
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["managed", "effective_controller"])
    );
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(false));
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));

    let mixed_case_cursor = cursor
        .chars()
        .enumerate()
        .map(|(index, value)| {
            if index % 2 == 0 {
                value.to_ascii_uppercase()
            } else {
                value
            }
        })
        .collect::<String>();
    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "page_size": 1,
                    "cursor": mixed_case_cursor
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["managed", "effective_controller"])
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 8453,
                    "roles": ["owned", "managed"],
                    "page_size": 1
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(payload["results"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        payload["results"][0]["records"][0]["primary_address"],
        json!("0x0000000000000000000000000000000000000def")
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "owned",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"]
                },
                {
                    "id": "managed",
                    "kind": "address",
                    "address": managed,
                    "coin_type": 60,
                    "roles": ["owned", "managed"]
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["input"]["address"], json!(address));
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(1));
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(payload["results"][1]["input"]["address"], json!(managed));
    assert_eq!(
        payload["results"][1]["records"]
            .as_array()
            .expect("records must be array")
            .len(),
        0
    );
    assert_eq!(payload["results"][1]["page"]["total_count"], json!(0));
    assert_eq!(payload["results"][1]["status"], json!("success"));

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [
                {
                    "id": "owned",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"]
                },
                {
                    "id": "managed",
                    "kind": "address",
                    "address": managed,
                    "coin_type": 60,
                    "roles": ["owned", "managed"]
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["input"]["address"], json!(address));
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(payload["results"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["owned"])
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));
    assert_eq!(payload["results"][0]["status"], json!("success"));
    assert_eq!(payload["results"][1]["input"]["address"], json!(managed));
    assert_eq!(
        payload["results"][1]["records"]
            .as_array()
            .expect("records must be array")
            .len(),
        0
    );
    assert_eq!(payload["results"][1]["page"]["total_count"], json!(0));
    assert_eq!(payload["results"][1]["status"], json!("success"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_reverse_cursor_applies_after_relation_deduplication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_identity_name(
        &database,
        "ens:dual.eth",
        "Dual.eth",
        "dual.eth",
        "namehash:dual.eth",
        Uuid::from_u128(0x2d0301),
        Uuid::from_u128(0x2d0302),
        Uuid::from_u128(0x2d0303),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        61,
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:dual.eth",
            bigname_storage::AddressNameRelation::EffectiveController,
            "Dual.eth",
            "dual.eth",
            "namehash:dual.eth",
            Uuid::from_u128(0x2d0303),
            Uuid::from_u128(0x2d0301),
            Some(Uuid::from_u128(0x2d0302)),
            62,
        )],
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:zulu.eth",
        "Zulu.eth",
        "zulu.eth",
        "namehash:zulu.eth",
        Uuid::from_u128(0x2d0311),
        Uuid::from_u128(0x2d0312),
        Uuid::from_u128(0x2d0313),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        63,
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "page_size": 1
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("dual.eth"));
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["owned", "managed", "effective_controller"])
    );
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(true));
    let cursor = payload["results"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include cursor");

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "page_size": 1,
                    "cursor": cursor
                }
            ]
        }),
    )
    .await?;
    assert_eq!(payload["results"][0]["records"][0]["name"], json!("zulu.eth"));
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(false));
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_reverse_returns_primary_names_across_namespaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    seed_identity_name(
        &database,
        "ens:alpha.eth",
        "Alpha.eth",
        "alpha.eth",
        "namehash:alpha.eth",
        Uuid::from_u128(0x2d0101),
        Uuid::from_u128(0x2d0102),
        Uuid::from_u128(0x2d0103),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        51,
    )
    .await?;
    seed_identity_name(
        &database,
        "basenames:beta.base.eth",
        "beta.base.eth",
        "beta.base.eth",
        "namehash:beta.base.eth",
        Uuid::from_u128(0x2d0111),
        Uuid::from_u128(0x2d0112),
        Uuid::from_u128(0x2d0113),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        52,
    )
    .await?;
    bigname_storage::upsert_primary_name_current_snapshots(
        &database.pool,
        &[
            bigname_storage::PrimaryNameCurrentSnapshot {
                row: bigname_storage::PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                    raw_claim_name: None,
                    claim_provenance: json!({"source": "identity_test"}),
                },
                normalized_claim_name: Some("alpha.eth".to_owned()),
            },
            bigname_storage::PrimaryNameCurrentSnapshot {
                row: bigname_storage::PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "basenames".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: bigname_storage::PrimaryNameClaimStatus::Success,
                    raw_claim_name: None,
                    claim_provenance: json!({"source": "identity_test"}),
                },
                normalized_claim_name: Some("beta.base.eth".to_owned()),
            },
        ],
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned", "managed"],
                    "page_size": 10
                }
            ]
        }),
    )
    .await?;
    let records = payload["results"][0]["records"]
        .as_array()
        .expect("records must be an array");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], json!("alpha.eth"));
    assert_eq!(records[0]["network"], json!("ethereum"));
    assert_eq!(records[0]["is_primary"], json!(true));
    assert_eq!(records[1]["name"], json!("beta.base.eth"));
    assert_eq!(records[1]["network"], json!("base"));
    assert_eq!(records[1]["is_primary"], json!(true));
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(false));
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(2));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_reverse_paginates_only_reachable_name_records() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    database
        .seed_name_current_binding_migrated(
            "ens:aaa-missing.eth",
            Uuid::from_u128(0x2d0151),
            Uuid::from_u128(0x2d0152),
            Uuid::from_u128(0x2d0153),
        )
        .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:aaa-missing.eth",
            bigname_storage::AddressNameRelation::Registrant,
            "aaa-missing.eth",
            "aaa-missing.eth",
            "namehash:aaa-missing.eth",
            Uuid::from_u128(0x2d0153),
            Uuid::from_u128(0x2d0151),
            Some(Uuid::from_u128(0x2d0152)),
            53,
        )],
    )
    .await?;
    database
        .seed_name_current_binding_migrated(
            "ens:aab-unreadable.eth",
            Uuid::from_u128(0x2d0171),
            Uuid::from_u128(0x2d0172),
            Uuid::from_u128(0x2d0173),
        )
        .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:aab-unreadable.eth",
            bigname_storage::AddressNameRelation::Registrant,
            "aab-unreadable.eth",
            "aab-unreadable.eth",
            "namehash:aab-unreadable.eth",
            Uuid::from_u128(0x2d0173),
            Uuid::from_u128(0x2d0171),
            Some(Uuid::from_u128(0x2d0172)),
            54,
        )],
    )
    .await?;
    let unreadable_resource_id = Uuid::from_u128(0x2d0181);
    let unreadable_binding_id = Uuid::from_u128(0x2d0183);
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ethereum-mainnet',
            '0xidentity-unreadable-resource',
            55,
            'orphaned'
        )
        "#,
    )
    .bind(unreadable_resource_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ens:aab-unreadable.eth',
            $2,
            'declared_registry_path',
            '2026-04-17T00:00:55Z',
            'ethereum-mainnet',
            '0xidentity-unreadable-binding',
            55,
            'orphaned'
        )
        "#,
    )
    .bind(unreadable_binding_id)
    .bind(unreadable_resource_id)
    .execute(&database.pool)
    .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:aab-unreadable.eth",
            "aab-unreadable.eth",
            "aab-unreadable.eth",
            "namehash:aab-unreadable.eth",
            unreadable_binding_id,
            unreadable_resource_id,
            None,
            55,
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
    seed_identity_name(
        &database,
        "ens:reachable.eth",
        "Reachable.eth",
        "reachable.eth",
        "namehash:reachable.eth",
        Uuid::from_u128(0x2d0161),
        Uuid::from_u128(0x2d0162),
        Uuid::from_u128(0x2d0163),
        address,
        bigname_storage::AddressNameRelation::Registrant,
        56,
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"],
                    "page_size": 1
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"][0]["name"],
        json!("reachable.eth")
    );
    assert_eq!(payload["results"][0]["page"]["has_more"], json!(false));
    assert_eq!(payload["results"][0]["page"]["next_cursor"], Value::Null);
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(1));

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"]
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"][0]["name"],
        json!("reachable.eth")
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(1));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn identity_reverse_total_count_tracks_visible_rows_and_relation_updates() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let resource_id = Uuid::from_u128(0x2d0201);

    seed_identity_name(
        &database,
        "ens:counted.eth",
        "Counted.eth",
        "counted.eth",
        "namehash:counted.eth",
        resource_id,
        Uuid::from_u128(0x2d0202),
        Uuid::from_u128(0x2d0203),
        address,
        bigname_storage::AddressNameRelation::Registrant,
        53,
    )
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"],
                    "page_size": 10
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"][0]["name"],
        json!("counted.eth")
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(1));

    sqlx::query(
        r#"
        UPDATE address_names_current
        SET relation = 'token_holder'
        WHERE address = $1
          AND logical_name_id = 'ens:counted.eth'
          AND relation = 'registrant'
        "#,
    )
    .bind(address)
    .execute(&database.pool)
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"],
                    "page_size": 10
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"][0]["relation_facets"],
        json!(["owned"])
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(1));

    sqlx::query(
        r#"
        UPDATE resources
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(&database.pool)
    .await?;

    let payload = identity_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [
                {
                    "id": "addr",
                    "kind": "address",
                    "address": address,
                    "coin_type": 60,
                    "roles": ["owned"],
                    "page_size": 10
                }
            ]
        }),
    )
    .await?;
    assert_eq!(
        payload["results"][0]["records"]
            .as_array()
            .expect("records must be array")
            .len(),
        0
    );
    assert_eq!(payload["results"][0]["page"]["total_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_degrades_without_chain_readiness_data() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("empty indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("degraded"));
    assert_eq!(
        payload["data"]["chains"]
            .as_object()
            .expect("chains must be an object")
            .len(),
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_degrades_for_chain_without_checkpoint() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (chain_id)
        VALUES ('ethereum-mainnet')
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing checkpoint indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("degraded"));
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["canonical_block"],
        Value::Null
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["latest_projected_block"],
        Value::Null
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_degrades_for_active_or_shadow_manifest_without_checkpoint() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_manifest(
            "basenames",
            "basenames_base_registry_shadow",
            "base-sepolia",
            "basenames_shadow",
            1,
            "shadow",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-only indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("degraded"));
    assert_eq!(
        payload["data"]["chains"]["base-mainnet"]["canonical_block"],
        Value::Null
    );
    assert_eq!(
        payload["data"]["chains"]["base-mainnet"]["latest_projected_block"],
        Value::Null
    );
    assert_eq!(
        payload["data"]["chains"]["base-sepolia"]["canonical_block"],
        Value::Null
    );
    assert_eq!(
        payload["data"]["chains"]["base-sepolia"]["latest_projected_block"],
        Value::Null
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_degrades_for_direct_pending_invalidations() -> Result<()> {
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
        VALUES (
            'ethereum-mainnet',
            '0xstatusdirect10',
            10,
            '2026-04-17T00:00:10Z',
            'canonical'
        )
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
            '0xstatusdirect10',
            10,
            '0xstatusdirect10',
            10,
            '0xstatusdirect10',
            10
        )
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES (
            'record_inventory_current',
            'direct:resolver',
            '{"source": "direct_test"}'::jsonb
        )
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("direct invalidation indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("degraded"));
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(10)
    );

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
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("stale"));
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["canonical_block"],
        json!(10)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(9)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["projection_lag_blocks"],
        json!(1)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["projection_lag_seconds"],
        json!(1)
    );

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(&database.pool)
        .await?;
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ready indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("ready"));
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(10)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["projection_lag_blocks"],
        json!(0)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn indexing_status_does_not_stale_unaffected_chain_for_global_cursor_lag() -> Result<()> {
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
            ('base-mainnet', '0xbase100', 100, '2026-04-17T00:01:40Z', 'canonical'),
            ('base-mainnet', '0xbase110', 110, '2026-04-17T00:01:50Z', 'canonical'),
            ('ethereum-mainnet', '0xeth10', 10, '2026-04-17T00:00:10Z', 'canonical'),
            ('ethereum-mainnet', '0xeth11', 11, '2026-04-17T00:00:11Z', 'canonical')
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
        VALUES
            (
                'base-mainnet',
                '0xbase110',
                110,
                '0xbase110',
                110,
                '0xbase110',
                110
            ),
            (
                'ethereum-mainnet',
                '0xeth11',
                11,
                '0xeth10',
                10,
                '0xeth10',
                10
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
        VALUES
            (
                'status-base-event-100',
                'basenames',
                'basenames:status.base.eth',
                'NameRegistered',
                'basenames_base_registry',
                1,
                'base-mainnet',
                100,
                '0xbase100',
                '{}'::jsonb,
                'status-test',
                'canonical',
                '{}'::jsonb,
                '{}'::jsonb,
                '2026-04-17T00:01:40Z'
            ),
            (
                'status-eth-event-10',
                'ens',
                'ens:status.eth',
                'NameRegistered',
                'ens_v1_registry_l1',
                1,
                'ethereum-mainnet',
                10,
                '0xeth10',
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
            '2026-04-17T00:01:40Z'
        FROM projection_normalized_event_changes
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
            'status-eth-event-11',
            'ens',
            'ens:status.eth',
            'ResolverChanged',
            'ens_v1_registry_l1',
            1,
            'ethereum-mainnet',
            11,
            '0xeth11',
            '{}'::jsonb,
            'status-test',
            'canonical',
            '{}'::jsonb,
            '{}'::jsonb,
            '2026-04-17T00:00:11Z'
        )
        "#,
    )
    .execute(&database.pool)
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("cross-chain cursor-lag indexing status request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["status"], json!("stale"));
    assert_eq!(
        payload["data"]["chains"]["base-mainnet"]["latest_projected_block"],
        json!(110)
    );
    assert_eq!(
        payload["data"]["chains"]["base-mainnet"]["projection_lag_blocks"],
        json!(0)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["latest_projected_block"],
        json!(10)
    );
    assert_eq!(
        payload["data"]["chains"]["ethereum-mainnet"]["projection_lag_blocks"],
        json!(1)
    );

    database.cleanup().await?;
    Ok(())
}

async fn identity_lookup_json(database: &TestDatabase, body: Value) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/identity:lookup")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&body).expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("native identity lookup request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn set_name_surface_labelhashes(
    database: &TestDatabase,
    logical_name_id: &str,
    labelhashes: &[&str],
) -> Result<()> {
    let labelhashes = labelhashes
        .iter()
        .map(|labelhash| (*labelhash).to_owned())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET labelhashes = $2
        WHERE logical_name_id = $1
        "#,
    )
    .bind(logical_name_id)
    .bind(labelhashes)
    .execute(&database.pool)
    .await?;
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
