#[tokio::test]
async fn get_resolvers_compact_overview_returns_projection_backed_summary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain_id = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000aaa";

    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row(chain_id, resolver_address)],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA/overview")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload,
        json!({
            "data": {
                "chain_id": chain_id,
                "resolver_address": resolver_address,
                "counts": {
                    "nodes": 2,
                    "aliases": 1,
                    "role_holders": 1,
                    "events": 2,
                },
                "nodes": [
                    {
                        "namespace": "ens",
                        "name": "Alice.eth",
                        "normalized_name": "alice.eth",
                        "namehash": "namehash:alice.eth",
                    },
                    {
                        "namespace": "ens",
                        "name": "Beta.eth",
                        "normalized_name": "beta.eth",
                        "namehash": "namehash:beta.eth",
                    }
                ],
                "aliases": [{
                    "namespace": "ens",
                    "name": "Beta.eth",
                    "normalized_name": "beta.eth",
                    "namehash": "namehash:beta.eth",
                }],
                "roles": [{
                    "subject": "0x0000000000000000000000000000000000000abc",
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records", "set_resolver"],
                    "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                }],
                "events": null,
            },
            "meta": {
                "support_status": "partial",
                "unsupported_filters": [],
                "unsupported_fields": ["events"],
            },
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolvers_compact_overview_keeps_unprojected_sections_explicit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain_id = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000d44";

    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[ResolverCurrentRow {
            chain_id: chain_id.to_owned(),
            resolver_address: resolver_address.to_owned(),
            declared_summary: json!({
                "bindings": {
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                },
                "aliases": {
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                },
                "role_holders": {
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                },
                "event_summary": {
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                },
            }),
            provenance: json!({
                "normalized_event_ids": [1201],
                "raw_fact_refs": [{
                    "kind": "raw_log",
                    "chain_id": chain_id,
                    "block_number": 21_000_044,
                }],
                "manifest_versions": [{
                    "manifest_version": 7,
                    "source_family": "ens_v1_resolver_l1",
                    "chain": chain_id,
                    "deployment_epoch": "ens_v1",
                }],
                "execution_trace_id": null,
                "derivation_kind": "resolver_current_rebuild",
            }),
            coverage: json!({
                "status": "partial",
                "exhaustiveness": "best_effort",
                "source_classes_considered": ["ens_v1_resolver_l1"],
                "unsupported_reason": "resolver_family_pending",
                "enumeration_basis": "resolver_target",
            }),
            chain_positions: json!({
                "ethereum": {
                    "chain_id": chain_id,
                    "block_number": 21_000_044,
                    "block_hash": "0xdynamicresolverpending",
                    "timestamp": "2026-04-17T00:00:44Z",
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    chain_id: "finalized",
                }
            }),
            manifest_version: 7,
            last_recomputed_at: timestamp(1_748_800_244),
        }],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/resolvers/{chain_id}/{resolver_address}/overview"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported compact resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["counts"], json!({}));
    assert_eq!(payload["data"]["nodes"], Value::Null);
    assert_eq!(payload["data"]["aliases"], Value::Null);
    assert_eq!(payload["data"]["roles"], Value::Null);
    assert_eq!(payload["data"]["events"], Value::Null);
    assert!(payload["data"].get("unsupported_sections").is_none());
    assert_eq!(payload["meta"]["support_status"], json!("unsupported"));
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["nodes", "aliases", "roles", "events"])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolvers_compact_overview_honors_include_and_meta_none() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain_id = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000aaa";

    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row(chain_id, resolver_address)],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000aaa/overview?include=nodes&meta=none")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("included compact resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: Value = read_json(response).await?;
    assert!(payload.get("meta").is_none());
    assert!(payload["data"].get("nodes").is_some());
    assert!(payload["data"].get("aliases").is_none());
    assert!(payload["data"].get("roles").is_none());
    assert!(payload["data"].get("events").is_none());
    assert_eq!(
        payload["data"]["counts"],
        json!({
            "nodes": 2,
            "aliases": 1,
            "role_holders": 1,
            "events": 2,
        })
    );
    assert!(payload["data"].get("unsupported_sections").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolvers_compact_overview_rejects_view_full() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let chain_id = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000aaa";

    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row(chain_id, resolver_address)],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000aaa/overview?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("full resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "view=full is not supported for compact resolver overview"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolvers_compact_overview_rejects_unknown_include() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000aaa/overview?include=nodes,records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid include compact resolver overview request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "include must contain only nodes, aliases, roles, or events"
    );

    database.cleanup().await?;
    Ok(())
}
