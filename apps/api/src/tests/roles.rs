#[tokio::test]
async fn resource_lookup_resolves_name_current_resource_identity() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xd001);
    let token_lineage_id = Uuid::from_u128(0xd002);
    let surface_binding_id = Uuid::from_u128(0xd003);

    database
        .seed_name_current_binding_migrated(
            "ens:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            "ens:alice.eth",
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resources/lookup?namespace=ens&name=alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource lookup request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!({
            "namespace": "ens",
            "name": "Alice.eth",
            "normalized_name": "alice.eth",
            "resource_id": resource_id.to_string(),
            "resource_hex": null,
        })
    );
    assert_eq!(payload["meta"]["support_status"], json!("supported"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["resource_hex"])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn roles_filter_by_account_resource_and_name_from_permissions_current() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let account = "0x0000000000000000000000000000000000000aaa";
    let other_account = "0x0000000000000000000000000000000000000bbb";
    let alice_resource_id = Uuid::from_u128(0xd101);
    let alice_token_lineage_id = Uuid::from_u128(0xd102);
    let alice_surface_binding_id = Uuid::from_u128(0xd103);
    let beta_resource_id = Uuid::from_u128(0xd104);

    database
        .seed_name_current_binding_migrated(
            "ens:alice.eth",
            alice_resource_id,
            alice_token_lineage_id,
            alice_surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            "ens:alice.eth",
            alice_surface_binding_id,
            alice_resource_id,
            alice_token_lineage_id,
        ))
        .await?;
    bigname_storage::upsert_resources(&database.pool, &[resource(beta_resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(alice_resource_id, account, PermissionScope::Resource, 12, 41),
            permission_current_row(beta_resource_id, account, PermissionScope::Registry, 13, 42),
            permission_current_row(
                alice_resource_id,
                other_account,
                PermissionScope::Resource,
                14,
                43,
            ),
        ],
    )
    .await?;
    mark_permissions_current_projection_ready(&database).await?;

    let account_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/roles?account={account}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("account roles request failed")?;
    assert_eq!(account_response.status(), StatusCode::OK);
    let account_payload: Value = read_json(account_response).await?;
    assert_eq!(account_payload["data"].as_array().unwrap().len(), 2);
    assert_eq!(account_payload["page"]["sort"], json!("account_resource_scope_asc"));
    assert_eq!(account_payload["meta"]["total_count"], json!(2));
    assert_eq!(
        account_payload["data"][0]["resource_id"],
        json!(alice_resource_id.to_string())
    );
    assert_eq!(account_payload["data"][0]["account"], json!(account));
    assert_eq!(account_payload["data"][0]["name"], json!("Alice.eth"));
    assert_eq!(account_payload["data"][0]["resource_hex"], JsonValue::Null);
    assert_eq!(account_payload["data"][0]["role_bitmap"], JsonValue::Null);
    assert_eq!(
        account_payload["data"][0]["effective_powers"],
        json!(["set_resolver", "create_subnames"])
    );
    assert!(account_payload["data"][0].get("provenance").is_none());

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?account={account}&resource_id={alice_resource_id}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource-filtered roles request failed")?;
    assert_eq!(resource_response.status(), StatusCode::OK);
    let resource_payload: Value = read_json(resource_response).await?;
    assert_eq!(resource_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        resource_payload["data"][0]["resource_id"],
        json!(alice_resource_id.to_string())
    );

    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?account={account}&namespace=ens&name=alice.eth"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name-filtered roles request failed")?;
    assert_eq!(name_response.status(), StatusCode::OK);
    let name_payload: Value = read_json(name_response).await?;
    assert_eq!(name_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        name_payload["data"][0]["resource_id"],
        json!(alice_resource_id.to_string())
    );
    assert_eq!(name_payload["data"][0]["name"], json!("Alice.eth"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn name_roles_resolves_current_resource_and_paginates() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xd201);
    let token_lineage_id = Uuid::from_u128(0xd202);
    let surface_binding_id = Uuid::from_u128(0xd203);
    let first_account = "0x0000000000000000000000000000000000000a01";
    let second_account = "0x0000000000000000000000000000000000000b02";

    database
        .seed_name_current_binding_migrated(
            "ens:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            "ens:alice.eth",
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(resource_id, second_account, PermissionScope::Registry, 21, 52),
            permission_current_row(resource_id, first_account, PermissionScope::Resource, 22, 51),
        ],
    )
    .await?;
    mark_permissions_current_projection_ready(&database).await?;

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/roles?page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name roles first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: Value = read_json(first_page_response).await?;
    assert_eq!(first_page_payload["page"]["sort"], json!("account_scope_asc"));
    assert_eq!(first_page_payload["meta"]["total_count"], json!(2));
    assert_eq!(first_page_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(first_page_payload["data"][0]["account"], json!(first_account));
    let cursor = first_page_payload["page"]["next_cursor"]
        .as_str()
        .expect("first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/alice.eth/roles?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name roles second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: Value = read_json(second_page_response).await?;
    assert_eq!(second_page_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(second_page_payload["data"][0]["account"], json!(second_account));
    assert_eq!(second_page_payload["page"]["next_cursor"], JsonValue::Null);
    assert_eq!(second_page_payload["meta"]["total_count"], json!(2));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn roles_return_stale_until_permissions_projection_is_available() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let account = "0x0000000000000000000000000000000000000aaa";

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/roles?account={account}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles request before permissions projection failed")?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "stale");
    assert!(
        payload
            .error
            .message
            .contains("permissions_current projection is not yet available")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn roles_reject_unsupported_bitmap_filter_and_missing_primary_filter() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let bitmap_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/roles?role_bitmap=0x01")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("role bitmap request failed")?;
    assert_eq!(bitmap_response.status(), StatusCode::BAD_REQUEST);
    let bitmap_payload: ErrorResponse = read_json(bitmap_response).await?;
    assert_eq!(bitmap_payload.error.code, "unsupported");
    assert!(
        bitmap_payload
            .error
            .message
            .contains("permissions_current does not project raw role bitmaps")
    );

    let missing_filter_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/roles")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing roles filter request failed")?;
    assert_eq!(missing_filter_response.status(), StatusCode::BAD_REQUEST);
    let missing_payload: ErrorResponse = read_json(missing_filter_response).await?;
    assert_eq!(missing_payload.error.code, "invalid_input");
    assert!(
        missing_payload
            .error
            .message
            .contains("at least one of account, resource_id, or namespace+name")
    );

    database.cleanup().await?;
    Ok(())
}

async fn mark_permissions_current_projection_ready(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ('permissions_current', 1, 0, 0, 0, 0)
        ON CONFLICT (projection) DO UPDATE SET
            replay_version = EXCLUDED.replay_version,
            completed_normalized_target_block = EXCLUDED.completed_normalized_target_block,
            requested_key_count = EXCLUDED.requested_key_count,
            upserted_row_count = EXCLUDED.upserted_row_count,
            deleted_row_count = EXCLUDED.deleted_row_count,
            completed_at = now()
        "#,
    )
    .execute(&database.pool)
    .await
    .context("failed to mark permissions_current projection ready")?;

    Ok(())
}
