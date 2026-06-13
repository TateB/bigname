#[tokio::test]
async fn get_name_records_returns_declared_compact_summary() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?texts=com.twitter&known_text_keys=true&avatar=true&content_hash=true&coin_types=60,0&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/resolver_address"),
        Some(&json!("0x0000000000000000000000000000000000000abc"))
    );
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/value"),
        Some(&json!("@alice"))
    );
    assert!(
        payload
            .pointer("/data/text_records/com.twitter/inventory")
            .is_none()
    );
    assert_eq!(
        payload.pointer("/data/known_text_keys"),
        Some(&json!({
            "status": "supported",
            "keys": ["com.twitter"],
        }))
    );
    assert_eq!(payload.pointer("/data/avatar/status"), Some(&json!("success")));
    assert_eq!(
        payload.pointer("/data/avatar/value"),
        Some(&json!("ipfs://avatar"))
    );
    assert_eq!(
        payload.pointer("/data/content_hash/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/content_hash/value"),
        Some(&json!("ipfs://content"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x0000000000000000000000000000000000000abc"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/0/status"),
        Some(&json!("not_found"))
    );
    assert!(payload.pointer("/data/inventory_source").is_none());
    assert!(payload.pointer("/data/value_source").is_none());
    assert_eq!(
        payload.pointer("/meta"),
        Some(&json!({
            "support_status": "supported",
            "unsupported_filters": [],
            "unsupported_fields": [],
            "total_count": null,
            "value_source": {
                "mode": "declared",
                "declared_status": "supported",
                "source": "record_inventory_current",
            },
        }))
    );

    let lean_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?texts=com.twitter&coin_types=60&mode=declared&meta=none")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact records meta=none request failed")?;
    assert_eq!(lean_response.status(), StatusCode::OK);
    let lean_payload: Value = read_json(lean_response).await?;
    assert!(lean_payload.get("meta").is_none());
    assert!(lean_payload.pointer("/data/value_source").is_none());
    assert!(lean_payload.pointer("/data/inventory_source").is_none());
    assert!(
        lean_payload
            .pointer("/data/text_records/com.twitter/inventory")
            .is_none()
    );
    assert_eq!(
        lean_payload.pointer("/data/text_records/com.twitter/value"),
        Some(&json!("@alice"))
    );
    assert_eq!(lean_payload.pointer("/data/known_text_keys"), Some(&Value::Null));
    assert_eq!(lean_payload.pointer("/data/avatar"), Some(&Value::Null));
    assert_eq!(
        lean_payload.pointer("/data/content_hash"),
        Some(&Value::Null)
    );

    let keys_only_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=known_text_keys&mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact records known-text-keys-only request failed")?;
    assert_eq!(keys_only_response.status(), StatusCode::OK);
    let keys_only_payload: Value = read_json(keys_only_response).await?;
    assert_eq!(
        keys_only_payload.pointer("/data/text_records"),
        Some(&Value::Null),
        "known_text_keys-only requests must not emit text record values: {keys_only_payload:#}"
    );
    assert_eq!(
        keys_only_payload.pointer("/data/known_text_keys/keys"),
        Some(&json!(["com.twitter"]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_known_text_keys_only_does_not_request_values() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2208);
    let token_lineage_id = Uuid::from_u128(0x1108);
    let surface_binding_id = Uuid::from_u128(0x3308);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let mut inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    inventory.entries = json!([
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
                "value": "0x0000000000000000000000000000000000000abc",
            },
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://avatar",
            },
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://content",
            },
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "unsupported",
            "unsupported_reason": "value_not_retained_in_normalized_events",
        },
    ]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=known_text_keys&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact records known-text-key inventory request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.pointer("/data/text_records"), Some(&Value::Null));
    assert_eq!(
        payload.pointer("/data/known_text_keys/keys"),
        Some(&json!(["com.twitter"]))
    );
    assert_eq!(
        payload.pointer("/meta/unsupported_fields"),
        Some(&json!([]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_maps_declared_text_avatar_to_avatar_field() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2207);
    let token_lineage_id = Uuid::from_u128(0x1107);
    let surface_binding_id = Uuid::from_u128(0x3307);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let mut inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "cacheable": true,
    }]);
    inventory.entries = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "status": "success",
        "value": {
            "key": "avatar",
            "value": "https://cdn.example.test/alice.png",
        },
    }]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?avatar=true&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared compact text-avatar request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload.pointer("/data/avatar/status"), Some(&json!("success")));
    assert_eq!(
        payload.pointer("/data/avatar/value"),
        Some(&json!("https://cdn.example.test/alice.png"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_infers_basenames_namespace() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x2201);
    let token_lineage_id = Uuid::from_u128(0x1101);
    let surface_binding_id = Uuid::from_u128(0x3301);

    database
        .seed_name_current_binding(
            logical_name_id,
            "basenames",
            "alice.base.eth",
            "alice.base.eth",
            "namehash:alice.base.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.namespace = "basenames".to_owned();
    row.canonical_display_name = "alice.base.eth".to_owned();
    row.normalized_name = "alice.base.eth".to_owned();
    row.namehash = "namehash:alice.base.eth".to_owned();
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": "base-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        }
    });
    row.chain_positions = json!({
        "base-mainnet": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "base-mainnet": "finalized"
        }
    });
    database.insert_name_current_row(row).await?;
    let mut inventory =
        compact_records_inventory_current_row(logical_name_id, resource_id);
    inventory.record_version_boundary =
        basenames_dynamic_resolver_record_inventory_boundary(
            logical_name_id,
            resource_id,
            None,
            None,
        );
    inventory.chain_positions = json!({
        "base-mainnet": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });
    inventory.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "base-mainnet": "finalized"
        }
    });
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/basenames/alice.base.eth/records?texts=com.twitter&known_text_keys=true&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("inferred compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/value"),
        Some(&json!("@alice"))
    );
    assert_eq!(
        payload.pointer("/data/known_text_keys/keys"),
        Some(&json!(["com.twitter"]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_rejects_unnormalizable_name() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/bad%20name.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("inferred compact records invalid-name request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_defaults_auto_to_declared_all_available_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2210);
    let token_lineage_id = Uuid::from_u128(0x1110);
    let surface_binding_id = Uuid::from_u128(0x3310);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/value"),
        Some(&json!("@alice"))
    );
    assert_eq!(
        payload.pointer("/data/known_text_keys/keys"),
        Some(&json!(["com.twitter"]))
    );
    assert_eq!(
        payload.pointer("/data/avatar/value"),
        Some(&json!("ipfs://avatar"))
    );
    assert_eq!(
        payload.pointer("/data/content_hash/value"),
        Some(&json!("ipfs://content"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x0000000000000000000000000000000000000abc"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source"),
        Some(&json!({
            "mode": "auto",
            "declared_status": "supported",
            "source": "record_inventory_current",
        }))
    );
    assert_eq!(
        payload.pointer("/meta/unsupported_fields"),
        Some(&json!([]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_canonicalizes_inventory_coin_selectors_before_compact_coin_output(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2215);
    let token_lineage_id = Uuid::from_u128(0x1115);
    let surface_binding_id = Uuid::from_u128(0x3315);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([
        {
            "record_key": "addr:060",
            "record_family": "addr",
            "selector_key": "060",
            "cacheable": true,
        },
    ]);
    inventory.explicit_gaps = json!([]);
    inventory.entries = json!([
        {
            "record_key": "addr:060",
            "record_family": "addr",
            "selector_key": "060",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc",
            },
        },
    ]);
    database.insert_record_inventory_current_row(inventory).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=coins&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact records canonical inventory coin request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x0000000000000000000000000000000000000abc"))
    );
    assert!(
        payload.pointer("/data/coin_addresses/060").is_none(),
        "compact coin output must use canonical coin selector keys"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_returns_stale_when_default_snapshot_outruns_projection() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2211);
    let token_lineage_id = Uuid::from_u128(0x1111);
    let surface_binding_id = Uuid::from_u128(0x3311);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xlater-binding",
            Some("0xbinding"),
            21_000_004,
            1_713_331_204,
        )],
    )
    .await
    .context("failed to insert later lineage block for compact records stale test")?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[history_event(
            "api-test:compact-records-later-input",
            Some(logical_name_id),
            Some(resource_id),
            Some("ethereum-mainnet"),
            Some(21_000_004),
            Some("0xlater-binding"),
            Some("0xtx:compact-records-later-input"),
            Some(0),
            CanonicalityState::Canonical,
        )],
    )
    .await
    .context("failed to insert later projection input for compact records stale test")?;
    sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET
            canonical_block_hash = '0xlater-binding',
            canonical_block_number = 21_000_004,
            safe_block_hash = '0xlater-binding',
            safe_block_number = 21_000_004,
            finalized_block_hash = '0xlater-binding',
            finalized_block_number = 21_000_004
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .execute(&database.pool)
    .await
    .context("failed to move test chain checkpoint past projection")?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("selected-snapshot compact records request failed")?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "stale");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_auto_uses_verified_for_pending_resolver_profile() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2220);
    let token_lineage_id = Uuid::from_u128(0x1120);
    let surface_binding_id = Uuid::from_u128(0x3320);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d51";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000061);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000ee"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(
            dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            ),
        )
        .await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=coins&coin_types=60&mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto verified compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x00000000000000000000000000000000000000ee"))
    );
    assert!(
        payload
            .pointer("/data/coin_addresses/60/inventory")
            .is_none()
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/verified_status"),
        Some(&json!("supported"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_auto_uses_verified_when_declared_inventory_has_no_selectors(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2222);
    let token_lineage_id = Uuid::from_u128(0x1122);
    let surface_binding_id = Uuid::from_u128(0x3322);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000063);
    let fallback_record_keys = [
        "addr:60",
        "avatar",
        "contenthash",
        "text:description",
        "text:url",
        "text:email",
    ];
    let request_key = resolution_execution_request_key(&fallback_record_keys);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000f0"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "avatar",
            "status": "not_found",
            "failure_reason": "no_avatar_record"
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash_record"
        },
        {
            "record_key": "text:description",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "text:url",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "text:email",
            "status": "not_found",
            "failure_reason": "no_text_record"
        }
    ]);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([]);
    inventory.explicit_gaps = json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "gap_reason": "not_observed_on_current_resolver"
    }]);
    inventory.entries = json!([]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &fallback_record_keys,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto no-selector compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x00000000000000000000000000000000000000f0"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/verified_status"),
        Some(&json!("supported"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_auto_uses_verified_when_declared_cache_value_is_unretained(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2223);
    let token_lineage_id = Uuid::from_u128(0x1123);
    let surface_binding_id = Uuid::from_u128(0x3323);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000064);
    let request_key = resolution_execution_request_key(&["avatar"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "cacheable": true
    }]);
    inventory.explicit_gaps = json!([]);
    inventory.entries = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "status": "unsupported",
        "unsupported_reason": "value_not_retained_in_normalized_events"
    }]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=avatar&mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto unretained compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/avatar/value"),
        Some(&json!("https://cdn.example.test/alice.png"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_auto_probes_basic_verified_records_without_declared_inventory(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2221);
    let token_lineage_id = Uuid::from_u128(0x1121);
    let surface_binding_id = Uuid::from_u128(0x3321);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000062);
    let fallback_record_keys = [
        "addr:60",
        "avatar",
        "contenthash",
        "text:description",
        "text:url",
        "text:email",
    ];
    let request_key = resolution_execution_request_key(&fallback_record_keys);
    let mut persisted_verified_queries = Vec::new();
    for record_key in fallback_record_keys {
        persisted_verified_queries.push(match record_key {
            "addr:60" => json!({
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000ef"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }),
            "contenthash" => json!({
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": "ipfs://fallback-content"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }),
            "text:email" => json!({
                "record_key": "text:email",
                "status": "success",
                "value": {
                    "value": "nick@example.test"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }),
            record_key => json!({
                "record_key": record_key,
                "status": "not_found",
                "failure_reason": "no_record",
            }),
        });
    }
    let persisted_verified_queries = Value::Array(persisted_verified_queries);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &fallback_record_keys,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto basic fallback compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x00000000000000000000000000000000000000ef"))
    );
    assert_eq!(
        payload.pointer("/data/text_records/email/value"),
        Some(&json!("nick@example.test"))
    );
    assert!(payload.pointer("/data/text_records/description").is_none());
    assert_eq!(
        payload.pointer("/data/content_hash/value"),
        Some(&json!("ipfs://fallback-content"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/verified_status"),
        Some(&json!("supported"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolve_records_auto_probes_basic_verified_records_when_inventory_has_no_public_selectors(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2223);
    let token_lineage_id = Uuid::from_u128(0x1123);
    let surface_binding_id = Uuid::from_u128(0x3323);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000063);
    let fallback_record_keys = ["addr:60"];
    let request_key = resolution_execution_request_key(&fallback_record_keys);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000f0"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([
        {
            "record_key": "addr:18446744073709551616",
            "record_family": "addr",
            "selector_key": "18446744073709551616",
            "cacheable": true,
        }
    ]);
    inventory.entries = json!([
        {
            "record_key": "addr:18446744073709551616",
            "record_family": "addr",
            "selector_key": "18446744073709551616",
            "status": "not_found",
        }
    ]);
    database.insert_record_inventory_current_row(inventory).await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &fallback_record_keys,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?include=coins&mode=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("auto fallback compact records request with non-public inventory failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x00000000000000000000000000000000000000f0"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_returns_persisted_verified_compact_summary() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000060);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-verified"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000dd"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(compact_records_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?mode=verified&texts=com.twitter&avatar=true&coin_types=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/value"),
        Some(&json!("@alice-verified"))
    );
    assert!(
        payload
            .pointer("/data/text_records/com.twitter/inventory")
            .is_none()
    );
    assert_eq!(
        payload.pointer("/data/avatar/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/avatar/value"),
        Some(&json!("https://cdn.example.test/alice.png"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/status"),
        Some(&json!("success"))
    );
    assert_eq!(
        payload.pointer("/data/coin_addresses/60/value"),
        Some(&json!("0x00000000000000000000000000000000000000dd"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/declared_status"),
        Some(&json!("not_requested"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/verified_status"),
        Some(&json!("supported"))
    );
    assert_eq!(
        payload.pointer("/meta/value_source/source"),
        Some(&json!("verified_resolution"))
    );
    assert_eq!(
        payload.pointer("/meta/unsupported_fields"),
        Some(&json!([]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_surfaces_missing_inventory_as_unsupported_metadata() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?texts=com.twitter&known_text_keys=true&avatar=true")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing inventory compact records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/status"),
        Some(&json!("unsupported"))
    );
    assert_eq!(
        payload.pointer("/data/text_records/com.twitter/unsupported_reason"),
        Some(&json!("declared compact record cache is not yet projected"))
    );
    assert_eq!(
        payload.pointer("/data/known_text_keys"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared compact record inventory is not yet projected",
        }))
    );
    assert!(payload.pointer("/data/inventory_source").is_none());
    assert!(payload.pointer("/data/value_source").is_none());
    assert_eq!(
        payload.pointer("/meta/value_source/declared_status"),
        Some(&json!("unsupported"))
    );
    assert_eq!(
        payload.pointer("/meta"),
        Some(&json!({
            "support_status": "partial",
            "unsupported_filters": [],
            "unsupported_fields": ["record_cache", "record_inventory"],
            "total_count": null,
            "value_source": {
                "mode": "declared",
                "declared_status": "unsupported",
                "source": "record_inventory_current",
                "declared_unsupported_reason": "declared compact record cache is not yet projected",
            },
        }))
    );

    let lean_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?texts=com.twitter&known_text_keys=true&avatar=true&meta=none")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing inventory compact records meta=none request failed")?;
    assert_eq!(lean_response.status(), StatusCode::OK);
    let lean_payload: Value = read_json(lean_response).await?;
    assert!(lean_payload.get("meta").is_none());
    assert_eq!(lean_payload.pointer("/data/known_text_keys"), Some(&Value::Null));
    assert!(lean_payload.pointer("/data/value_source").is_none());
    assert!(lean_payload.pointer("/data/inventory_source").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_rejects_full_view_until_full_envelope_is_documented_here() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("full-view compact records request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "view=full is not supported for compact name records"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_rejects_canonical_duplicate_coin_types() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?coin_types=060,60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("canonical duplicate compact coin_types request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "coin_types must not contain duplicate selectors"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_rejects_exact_duplicate_coin_types() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?coin_types=60,60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("exact duplicate compact coin_types request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "coin_types must not contain duplicate selectors"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_records_rejects_overflowing_coin_types() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/records?coin_types=18446744073709551616")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("overflowing compact coin_types request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "coin_types must contain only u64 decimal coin-type selectors"
    );

    database.cleanup().await?;
    Ok(())
}

fn compact_records_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    let mut row = record_inventory_current_row(logical_name_id, resource_id);
    row.selectors = json!([
        {
            "record_key": "addr:0",
            "record_family": "addr",
            "selector_key": "0",
            "cacheable": true,
        },
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true,
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true,
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "cacheable": true,
        },
    ]);
    row.explicit_gaps = json!([]);
    row.entries = json!([
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
                "value": "0x0000000000000000000000000000000000000abc",
            },
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://avatar",
            },
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://content",
            },
        },
        {
            "record_key": "text:com.twitter",
            "record_family": "text",
            "selector_key": "com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "@alice",
            },
        },
    ]);
    row
}
