#[tokio::test]
async fn v2_lookup_rejects_invalid_request_shapes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for (uri, body) in [
        (
            "/v2/lookup",
            json!({"inputs": [{"id": "both", "name": "alice.eth", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
        ("/v2/lookup", json!({"inputs": [{"id": "neither"}]})),
        ("/v2/lookup", json!({"inputs": [{"id": "", "name": "alice.eth"}]})),
        (
            "/v2/lookup",
            json!({"profile": "detail", "extra": true, "inputs": []}),
        ),
        (
            "/v2/lookup",
            json!({"namespace": "ens", "inputs": [{"id": "addr", "address": "0x0000000000000000000000000000000000000abc"}]}),
        ),
    ] {
        let response = v2_lookup_response_for_database(&database, uri, body).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    for uri in [
        "/v2/lookup?at=2026-04-17T00:00:00Z",
        "/v2/lookup?finality=safe",
    ] {
        let response = v2_lookup_response_for_database(
            &database,
            uri,
            json!({"inputs": [{"id": "name", "name": "alice.eth"}]}),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"));
    }

    let oversized_inputs = (0..=1000)
        .map(|index| json!({"id": format!("name-{index}"), "name": "alice.eth"}))
        .collect::<Vec<_>>();
    let response = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
        json!({"inputs": oversized_inputs}),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_forward_results_are_in_order_with_head_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:case.eth",
        "Case.eth",
        "case.eth",
        "namehash:case.eth",
        Uuid::from_u128(0x5a0101),
        Uuid::from_u128(0x5a0102),
        Uuid::from_u128(0x5a0103),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "public",
            "inputs": [
                {"id": "hit", "name": "Case.eth"},
                {"id": "miss", "name": "missing.eth"},
                {"id": "bad", "name": "bad name.eth"}
            ]
        }),
    )
    .await?;

    assert!(payload.get("page").is_none());
    assert!(payload["data"].is_array());
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 38,
            "block_hash": "0xname26",
            "timestamp": "2026-04-17T00:00:38Z"
        })
    );

    assert_eq!(payload["data"][0]["input"], json!({"id": "hit", "name": "Case.eth"}));
    assert_eq!(payload["data"][0]["kind"], json!("name"));
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert_eq!(
        payload["data"][0]["normalization"],
        json!({
            "changed": true,
            "input_name": "Case.eth",
            "reason": "case_normalized"
        })
    );
    assert_eq!(payload["data"][0]["record"]["name"], json!("case.eth"));
    assert_eq!(payload["data"][0]["record"]["display_name"], json!("Case.eth"));
    assert_eq!(payload["data"][0]["record"]["namespace"], json!("ens"));
    assert_eq!(payload["data"][0]["record"]["status"], json!("ok"));
    assert_eq!(
        payload["data"][0]["record"]["addresses"]["60"],
        json!(address)
    );
    assert_eq!(payload["data"][0]["record"]["primary_address"], json!(address));
    assert!(payload["data"][0].get("records").is_none());

    let omitted_id = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "case.eth"}]}),
    )
    .await?;
    assert_eq!(omitted_id["data"][0]["input"], json!({"name": "case.eth"}));
    assert_eq!(omitted_id["data"][0]["status"], json!("ok"));

    assert_eq!(payload["data"][1]["input"]["id"], json!("miss"));
    assert_eq!(payload["data"][1]["status"], json!("not_found"));
    assert!(payload["data"][1].get("record").is_none());
    assert_eq!(payload["data"][2]["input"]["id"], json!("bad"));
    assert_eq!(payload["data"][2]["status"], json!("invalid_name"));
    assert_eq!(
        payload["data"][2]["normalization"]["reason"],
        json!("invalid_normalized_name")
    );

    let feed = v2_lookup_json(
        &database,
        json!({"profile": "feed", "inputs": [{"id": "feed", "name": "case.eth"}]}),
    )
    .await?;
    let feed_record = feed["data"][0]["record"]
        .as_object()
        .expect("feed record must be an object");
    assert_eq!(feed_record.get("name"), Some(&json!("case.eth")));
    assert!(feed_record.get("addresses").is_none());
    assert!(feed_record.get("owner").is_none());

    let detail = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "detail", "name": "case.eth"}]}),
    )
    .await?;
    let shadow = v2_lookup_json(
        &database,
        json!({"profile": "shadow", "inputs": [{"id": "detail", "name": "case.eth"}]}),
    )
    .await?;
    assert_eq!(shadow["data"][0]["record"], detail["data"][0]["record"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_detail_paginates_after_head_advance() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let first_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "addr",
                "address": address,
                "page_size": 1
            }]
        }),
    )
    .await?;

    assert_eq!(first_page["data"][0]["kind"], json!("address"));
    assert_eq!(first_page["data"][0]["status"], json!("ok"));
    assert_eq!(
        first_page["data"][0]["input"],
        json!({
            "id": "addr",
            "address": address,
            "coin_type": 60,
            "page_size": 1
        })
    );
    assert_eq!(first_page["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(first_page["data"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        first_page["data"][0]["records"][0]["relations"],
        json!(["owner"])
    );
    assert_eq!(first_page["data"][0]["page"]["cursor"], Value::Null);
    assert_eq!(first_page["data"][0]["page"]["page_size"], json!(1));
    assert_eq!(first_page["data"][0]["page"]["total_count"], json!(2));
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next_cursor");

    advance_v2_lookup_ethereum_head(&database, 43, "0xlookup-advanced").await?;

    let second_page = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "addr",
                "address": address,
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(
        second_page["data"][0]["page"]["cursor"],
        json!(cursor.to_ascii_lowercase())
    );
    assert_eq!(
        second_page["meta"]["as_of"]["1"],
        json!({
            "block_number": 43,
            "block_hash": "0xlookup-advanced",
            "timestamp": "2026-04-17T00:00:43Z"
        })
    );
    assert_eq!(second_page["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(
        second_page["data"][0]["records"][0]["relations"],
        json!(["manager"])
    );
    assert_eq!(second_page["data"][0]["page"]["has_more"], json!(false));
    assert_eq!(second_page["data"][0]["page"]["total_count"], json!(2));

    let mismatch = v2_lookup_response_for_database(
        &database,
        "/v2/lookup",
        json!({
            "profile": "detail",
            "inputs": [{
                "id": "wrong",
                "address": "0x0000000000000000000000000000000000000def",
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;
    assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(mismatch).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_omits_chain_with_missing_head_hash() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 77,
                "block_hash": "0xlookup-head",
                "timestamp": "2026-04-17T00:01:17Z"
            }
        }))
        .await?;
    drop_canonical_checkpoint_pair_check(&database).await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (chain_id, canonical_block_number)
        VALUES ('base-mainnet', 88)
        "#,
    )
    .execute(&database.pool)
    .await
    .context("failed to seed checkpoint row without a canonical hash")?;

    let payload = v2_lookup_json(
        &database,
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("not_found"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 77,
            "block_hash": "0xlookup-head",
            "timestamp": "2026-04-17T00:01:17Z"
        })
    );
    assert!(payload["meta"]["as_of"].get("8453").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_feed_miss_and_all_miss_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [
                {"id": "hit", "address": address, "relation": "owner"},
                {"id": "miss", "address": "0x0000000000000000000000000000000000000def"}
            ]
        }),
    )
    .await?;
    assert_eq!(payload["data"][0]["status"], json!("ok"));
    assert_eq!(payload["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(payload["data"][0]["records"][0]["is_primary"], json!(true));
    assert_eq!(
        payload["data"][0]["records"][0]["relations"],
        json!(["owner"])
    );
    assert!(payload["data"][0]["records"][0].get("addresses").is_none());
    assert_eq!(payload["data"][0]["page"]["page_size"], json!(50));
    assert_eq!(payload["data"][0]["page"]["total_count"], Value::Null);
    assert_eq!(payload["data"][1]["status"], json!("ok"));
    assert_eq!(payload["data"][1]["records"], json!([]));
    assert_eq!(payload["data"][1]["page"]["total_count"], json!(0));

    let empty_database = TestDatabase::new_migrated().await?;
    empty_database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 77,
                "block_hash": "0xlookup-head",
                "timestamp": "2026-04-17T00:01:17Z"
            }
        }))
        .await?;
    let miss_payload = v2_lookup_json(
        &empty_database,
        json!({"inputs": [{"id": "miss", "name": "missing.eth"}]}),
    )
    .await?;
    assert_eq!(miss_payload["data"][0]["status"], json!("not_found"));
    assert_eq!(
        miss_payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 77,
            "block_hash": "0xlookup-head",
            "timestamp": "2026-04-17T00:01:17Z"
        })
    );

    database.cleanup().await?;
    empty_database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_feed_uses_detail_pagination_semantics() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;

    let first_page = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [{
                "address": address,
                "page_size": 1
            }]
        }),
    )
    .await?;

    assert_eq!(first_page["data"][0]["input"], json!({
        "address": address,
        "coin_type": 60,
        "page_size": 1
    }));
    assert_eq!(first_page["data"][0]["records"][0]["name"], json!("alice.eth"));
    assert_eq!(first_page["data"][0]["page"]["has_more"], json!(true));
    assert_eq!(first_page["data"][0]["page"]["total_count"], json!(2));
    let cursor = first_page["data"][0]["page"]["next_cursor"]
        .as_str()
        .expect("feed first page must include next_cursor");

    let second_page = v2_lookup_json(
        &database,
        json!({
            "profile": "feed",
            "inputs": [{
                "address": address,
                "page_size": 1,
                "cursor": cursor
            }]
        }),
    )
    .await?;

    assert_eq!(second_page["data"][0]["records"][0]["name"], json!("bob.eth"));
    assert_eq!(second_page["data"][0]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_failed_record_sets_failed_result_and_reason() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_v2_lookup_reverse_fixture(&database, address).await?;
    sqlx::query(
        r#"
        UPDATE name_current
        SET coverage = $2::jsonb
        WHERE logical_name_id = $1
        "#,
    )
    .bind("ens:alice.eth")
    .bind(json!({
        "status": "failed",
        "failure_reason": "projection_read_failed"
    }))
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE name_current
        SET coverage = $2::jsonb
        WHERE logical_name_id = $1
        "#,
    )
    .bind("ens:bob.eth")
    .bind(json!({
        "status": "stale"
    }))
    .execute(&database.pool)
    .await?;

    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address
            }]
        }),
    )
    .await?;

    assert_eq!(payload["data"][0]["status"], json!("failed"));
    assert_eq!(
        payload["data"][0]["failure_reason"],
        json!("projection_read_failed")
    );
    assert_eq!(payload["data"][0]["records"][0]["status"], json!("failed"));
    assert_eq!(
        payload["data"][0]["records"][0]["failure_reason"],
        json!("projection_read_failed")
    );
    assert_eq!(payload["data"][0]["records"][1]["status"], json!("stale"));
    assert!(payload["data"][0].get("unsupported_reason").is_none());
    assert!(payload["data"][0]["records"][0].get("unsupported_reason").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_lookup_reverse_relation_filters_owner_and_registrant_exactly() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    seed_identity_name(
        &database,
        "ens:holder.eth",
        "holder.eth",
        "holder.eth",
        "namehash:holder.eth",
        Uuid::from_u128(0x5a0301),
        Uuid::from_u128(0x5a0302),
        Uuid::from_u128(0x5a0303),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        44,
    )
    .await?;
    seed_identity_name(
        &database,
        "ens:registrant.eth",
        "registrant.eth",
        "registrant.eth",
        "namehash:registrant.eth",
        Uuid::from_u128(0x5a0311),
        Uuid::from_u128(0x5a0312),
        Uuid::from_u128(0x5a0313),
        address,
        bigname_storage::AddressNameRelation::Registrant,
        45,
    )
    .await?;

    let owner = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "owner"
            }]
        }),
    )
    .await?;
    assert_eq!(owner["data"][0]["records"][0]["name"], json!("holder.eth"));
    assert_eq!(owner["data"][0]["records"][0]["relations"], json!(["owner"]));
    assert_eq!(owner["data"][0]["page"]["total_count"], Value::Null);

    let registrant = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "inputs": [{
                "address": address,
                "relation": "registrant"
            }]
        }),
    )
    .await?;
    assert_eq!(
        registrant["data"][0]["records"][0]["name"],
        json!("registrant.eth")
    );
    assert_eq!(
        registrant["data"][0]["records"][0]["relations"],
        json!(["registrant"])
    );
    assert_eq!(registrant["data"][0]["page"]["total_count"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

async fn advance_v2_lookup_ethereum_head(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
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
            $1,
            $2,
            $3::TIMESTAMPTZ,
            'finalized'::canonicality_state
        )
        ON CONFLICT (chain_id, block_hash) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            block_timestamp = EXCLUDED.block_timestamp,
            canonicality_state = EXCLUDED.canonicality_state
        "#,
    )
    .bind(block_hash)
    .bind(block_number)
    .bind(format!("2026-04-17T00:00:{:02}Z", block_number % 60))
    .execute(&database.pool)
    .await
    .context("failed to seed advanced lookup head lineage")?;

    sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET
            canonical_block_hash = $1,
            canonical_block_number = $2,
            safe_block_hash = $1,
            safe_block_number = $2,
            finalized_block_hash = $1,
            finalized_block_number = $2,
            updated_at = now()
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .bind(block_hash)
    .bind(block_number)
    .execute(&database.pool)
    .await
    .context("failed to advance lookup head checkpoint")?;

    Ok(())
}

async fn drop_canonical_checkpoint_pair_check(database: &TestDatabase) -> Result<()> {
    let constraint_name: String = sqlx::query_scalar(
        r#"
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'chain_checkpoints'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%canonical_block_hash%'
          AND pg_get_constraintdef(oid) LIKE '%canonical_block_number%'
        "#,
    )
    .fetch_one(&database.pool)
    .await
    .context("failed to find canonical checkpoint pair check")?;
    let escaped = constraint_name.replace('"', "\"\"");

    sqlx::query(&format!(
        r#"ALTER TABLE chain_checkpoints DROP CONSTRAINT "{escaped}""#
    ))
    .execute(&database.pool)
    .await
    .context("failed to drop canonical checkpoint pair check")?;

    Ok(())
}

async fn seed_v2_lookup_reverse_fixture(database: &TestDatabase, address: &str) -> Result<()> {
    seed_identity_name(
        database,
        "ens:alice.eth",
        "Alice.eth",
        "alice.eth",
        "namehash:alice.eth",
        Uuid::from_u128(0x5a0201),
        Uuid::from_u128(0x5a0202),
        Uuid::from_u128(0x5a0203),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        41,
    )
    .await?;
    seed_identity_name(
        database,
        "ens:bob.eth",
        "Bob.eth",
        "bob.eth",
        "namehash:bob.eth",
        Uuid::from_u128(0x5a0211),
        Uuid::from_u128(0x5a0212),
        Uuid::from_u128(0x5a0213),
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
                claim_provenance: json!({"source": "v2_lookup_test"}),
            },
            normalized_claim_name: Some("alice.eth".to_owned()),
        }],
    )
    .await?;
    Ok(())
}

async fn v2_lookup_json(database: &TestDatabase, body: Value) -> Result<Value> {
    let response = v2_lookup_response_for_database(database, "/v2/lookup", body).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_lookup_response_for_database(
    database: &TestDatabase,
    uri: &str,
    body: Value,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&body).expect("body must serialize"),
                ))
                .expect("request must build"),
        )
        .await
        .context("v2 lookup request failed")
}
