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

    let spaced_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resources/lookup?namespace=ens&name=%20alice.eth%20")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource lookup with whitespace-padded name failed")?;
    assert_eq!(spaced_response.status(), StatusCode::NOT_FOUND);
    let spaced_payload: ErrorResponse = read_json(spaced_response).await?;
    assert_eq!(spaced_payload.error.code, "not_found");

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
async fn roles_omit_associated_name_for_closed_surface_binding() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let account = "0x0000000000000000000000000000000000000aaa";
    let resource_id = Uuid::from_u128(0xd301);
    let token_lineage_id = Uuid::from_u128(0xd302);
    let surface_binding_id = Uuid::from_u128(0xd303);

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
    sqlx::query(
        r#"
        UPDATE surface_bindings
        SET active_to = $2
        WHERE surface_binding_id = $1
        "#,
    )
    .bind(surface_binding_id)
    .bind(timestamp(1_717_171_800))
    .execute(&database.pool)
    .await
    .context("failed to close roles test surface binding")?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[permission_current_row(
            resource_id,
            account,
            PermissionScope::Resource,
            16,
            61,
        )],
    )
    .await?;
    mark_permissions_current_projection_ready(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/roles?account={account}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("account roles request for closed binding failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(payload["data"][0]["resource_id"], json!(resource_id.to_string()));
    assert_eq!(payload["data"][0]["name"], JsonValue::Null);

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
    assert_eq!(
        first_page_payload["page"]["sort"],
        json!("account_resource_scope_asc")
    );
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
async fn name_roles_precompose_ensv2_root_fallback_permissions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registry_contract_instance_id = Uuid::from_u128(0xe201);
    let resource_id = Uuid::from_u128(0xe202);
    let root_resource_id = ensv2_registry_resource_id(
        "ethereum-mainnet",
        registry_contract_instance_id,
        ENSV2_ROOT_UPSTREAM_RESOURCE,
    );
    let token_lineage_id = Uuid::from_u128(0xe203);
    let surface_binding_id = Uuid::from_u128(0xe204);
    let local_account = "0x0000000000000000000000000000000000000a01";
    let root_account = "0x0000000000000000000000000000000000000902";
    let registry_address = "0x0000000000000000000000000000000000000eac";
    let upstream_resource =
        "0x00000000000000000000000000000000000000000000000000000000000073c0";

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
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            ensv2_registry_resource(
                resource_id,
                registry_contract_instance_id,
                registry_address,
                upstream_resource,
            ),
            ensv2_registry_resource(
                root_resource_id,
                registry_contract_instance_id,
                registry_address,
                ENSV2_ROOT_UPSTREAM_RESOURCE,
            ),
        ],
    )
    .await?;

    let mut root_grant =
        permission_current_row(root_resource_id, root_account, PermissionScope::Root, 24, 54);
    root_grant.grant_source = json!({
        "kind": "normalized_event",
        "event_kind": "RootPermissionChanged",
        "root_resource": true,
        "registry_address": registry_address,
    });
    root_grant.inheritance_path = json!([
        {
            "kind": "registry_root_fallback",
            "chain_id": "ethereum-mainnet",
            "registry_address": registry_address,
            "upstream_resource": ENSV2_ROOT_UPSTREAM_RESOURCE,
        }
    ]);
    root_grant.transfer_behavior = json!({});

    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(resource_id, local_account, PermissionScope::Resource, 22, 53),
            root_grant,
        ],
    )
    .await?;
    mark_permissions_current_projection_ready(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/roles")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name roles root fallback request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    let rows = payload["data"].as_array().expect("data must be an array");
    assert_eq!(payload["meta"]["total_count"], json!(2));
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row["account"] == json!(local_account)
            && row["resource_id"] == json!(resource_id.to_string())
            && row["name"] == json!("Alice.eth")
    }));
    assert!(rows.iter().any(|row| {
        row["account"] == json!(root_account)
            && row["resource_id"] == json!(root_resource_id.to_string())
            && row["name"] == JsonValue::Null
    }));

    let query_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/roles?namespace=ens&name=alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles query root fallback request failed")?;
    assert_eq!(query_response.status(), StatusCode::OK);
    let query_payload: Value = read_json(query_response).await?;
    let query_rows = query_payload["data"]
        .as_array()
        .expect("query data must be an array");
    assert_eq!(query_payload["meta"]["total_count"], json!(2));
    assert_eq!(query_rows.len(), 2);
    assert!(query_rows.iter().any(|row| {
        row["account"] == json!(local_account)
            && row["resource_id"] == json!(resource_id.to_string())
            && row["name"] == json!("Alice.eth")
    }));
    assert!(query_rows.iter().any(|row| {
        row["account"] == json!(root_account)
            && row["resource_id"] == json!(root_resource_id.to_string())
            && row["name"] == JsonValue::Null
    }));

    let account_filtered_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/names/ens/alice.eth/roles?account={root_account}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name roles account-filtered root fallback request failed")?;
    assert_eq!(account_filtered_response.status(), StatusCode::OK);
    let account_filtered_payload: Value = read_json(account_filtered_response).await?;
    let account_filtered_rows = account_filtered_payload["data"]
        .as_array()
        .expect("query data must be an array");
    assert_eq!(account_filtered_payload["meta"]["total_count"], json!(1));
    assert_eq!(account_filtered_rows.len(), 1);
    assert_eq!(account_filtered_rows[0]["account"], json!(root_account));
    assert_eq!(
        account_filtered_rows[0]["resource_id"],
        json!(root_resource_id.to_string())
    );
    assert_eq!(account_filtered_rows[0]["name"], JsonValue::Null);

    let unrelated_resource_id = Uuid::from_u128(0xe299);
    let unrelated_filtered_query_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?namespace=ens&name=alice.eth&resource_id={unrelated_resource_id}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles query unrelated resource filter request failed")?;
    assert_eq!(unrelated_filtered_query_response.status(), StatusCode::OK);
    let unrelated_filtered_query_payload: Value = read_json(unrelated_filtered_query_response).await?;
    assert_eq!(unrelated_filtered_query_payload["meta"]["total_count"], json!(0));
    assert_eq!(
        unrelated_filtered_query_payload["data"]
            .as_array()
            .expect("query data must be an array")
            .len(),
        0
    );

    let resource_filtered_query_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?namespace=ens&name=alice.eth&resource_id={resource_id}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles query resource filter request failed")?;
    assert_eq!(resource_filtered_query_response.status(), StatusCode::OK);
    let resource_filtered_query_payload: Value = read_json(resource_filtered_query_response).await?;
    let resource_filtered_query_rows = resource_filtered_query_payload["data"]
        .as_array()
        .expect("query data must be an array");
    assert_eq!(resource_filtered_query_payload["meta"]["total_count"], json!(1));
    assert_eq!(resource_filtered_query_rows.len(), 1);
    assert_eq!(resource_filtered_query_rows[0]["account"], json!(local_account));
    assert_eq!(
        resource_filtered_query_rows[0]["resource_id"],
        json!(resource_id.to_string())
    );
    assert_eq!(resource_filtered_query_rows[0]["name"], json!("Alice.eth"));

    let root_filtered_query_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?namespace=ens&name=alice.eth&resource_id={root_resource_id}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles query root fallback resource filter request failed")?;
    assert_eq!(root_filtered_query_response.status(), StatusCode::OK);
    let root_filtered_query_payload: Value = read_json(root_filtered_query_response).await?;
    let root_filtered_query_rows = root_filtered_query_payload["data"]
        .as_array()
        .expect("query data must be an array");
    assert_eq!(root_filtered_query_payload["meta"]["total_count"], json!(1));
    assert_eq!(root_filtered_query_rows.len(), 1);
    assert_eq!(root_filtered_query_rows[0]["account"], json!(root_account));
    assert_eq!(
        root_filtered_query_rows[0]["resource_id"],
        json!(root_resource_id.to_string())
    );
    assert_eq!(root_filtered_query_rows[0]["name"], JsonValue::Null);

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth/roles?page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name roles root fallback first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: Value = read_json(first_page_response).await?;
    assert_eq!(first_page_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(first_page_payload["data"][0]["account"], json!(root_account));
    assert_eq!(
        first_page_payload["data"][0]["resource_id"],
        json!(root_resource_id.to_string())
    );
    let cursor = first_page_payload["page"]["next_cursor"]
        .as_str()
        .expect("first composed page must include next_cursor");

    let filtered_cursor_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/roles?namespace=ens&name=alice.eth&resource_id={resource_id}&page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles query filtered cursor replay request failed")?;
    assert_eq!(filtered_cursor_response.status(), StatusCode::BAD_REQUEST);
    let filtered_cursor_payload: ErrorResponse = read_json(filtered_cursor_response).await?;
    assert_eq!(filtered_cursor_payload.error.code, "invalid_input");

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
        .context("name roles root fallback second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: Value = read_json(second_page_response).await?;
    assert_eq!(second_page_payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(second_page_payload["data"][0]["account"], json!(local_account));
    assert_eq!(
        second_page_payload["data"][0]["resource_id"],
        json!(resource_id.to_string())
    );
    assert_eq!(second_page_payload["page"]["next_cursor"], JsonValue::Null);

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
async fn roles_treat_stale_permissions_replay_marker_as_unavailable() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let account = "0x0000000000000000000000000000000000000aaa";

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
        "#,
    )
    .execute(&database.pool)
    .await
    .context("failed to insert stale permissions_current replay marker")?;

    let stale_marker_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/roles?account={account}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles request with stale permissions marker failed")?;
    assert_eq!(stale_marker_response.status(), StatusCode::CONFLICT);
    let stale_marker_payload: ErrorResponse = read_json(stale_marker_response).await?;
    assert_eq!(stale_marker_payload.error.code, "stale");

    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (
            cursor_name,
            last_change_id
        )
        VALUES ('normalized_events_to_projection_invalidations', 1)
        "#,
    )
    .execute(&database.pool)
    .await
    .context("failed to insert normalized-event projection apply cursor")?;

    let cursor_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/roles?account={account}"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("roles request with stale marker and apply cursor failed")?;
    assert_eq!(cursor_response.status(), StatusCode::CONFLICT);
    let cursor_payload: ErrorResponse = read_json(cursor_response).await?;
    assert_eq!(cursor_payload.error.code, "stale");

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
        VALUES ('permissions_current', 5, 0, 0, 0, 0)
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

const ENSV2_ROOT_UPSTREAM_RESOURCE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

fn ensv2_registry_resource_id(
    chain_id: &str,
    registry_contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    let seed = format!(
        "ens-v2-resource:{chain_id}:{registry_contract_instance_id}:{upstream_resource}"
    );
    let digest = alloy_primitives::keccak256(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn ensv2_registry_resource(
    resource_id: Uuid,
    registry_contract_instance_id: Uuid,
    registry_address: &str,
    upstream_resource: &str,
) -> Resource {
    let mut resource = resource(resource_id);
    resource.provenance = json!({
        "adapter": "ens_v2_registry_resource_surface",
        "chain_id": "ethereum-mainnet",
        "registry_contract_instance_id": registry_contract_instance_id,
        "registry_address": registry_address,
        "upstream_resource": upstream_resource,
        "source_family": "ens_v2_registry_l1",
        "source_manifest_id": "ens-v2-registry-l1:ethereum-mainnet",
        "manifest_version": 1,
    });
    resource
}
