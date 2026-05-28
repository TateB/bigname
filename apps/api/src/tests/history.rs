#[tokio::test]
async fn get_name_history_returns_canonical_only_rows_with_provenance_and_coverage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa001);
    let surface_binding_id = Uuid::from_u128(0xb001);
    let manifest_id_v7 = database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "bootstrap",
            7,
            "active",
            "history-test-v1",
        )
        .await?;
    let manifest_id_v8 = database
        .insert_manifest(
            "ens",
            "ens_v1_registry_l1",
            "ethereum-mainnet",
            "bootstrap-next",
            8,
            "active",
            "history-test-v2",
        )
        .await?;

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
            raw_block(
                "ethereum-mainnet",
                "0x101",
                Some("0x100"),
                101,
                1_700_000_101,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x102",
                Some("0x101"),
                102,
                1_700_000_102,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x103",
                Some("0x102"),
                103,
                1_700_000_103,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                manifest_version: 7,
                source_manifest_id: Some(manifest_id_v7),
                ..history_event(
                    "history:canonical",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(100),
                    Some("0x100"),
                    Some("0xtx100"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                manifest_version: 8,
                source_manifest_id: Some(manifest_id_v8),
                ..history_event(
                    "history:safe",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(101),
                    Some("0x101"),
                    Some("0xtx101"),
                    Some(0),
                    CanonicalityState::Safe,
                )
            },
            NormalizedEvent {
                manifest_version: 7,
                source_manifest_id: Some(manifest_id_v7),
                ..history_event(
                    "history:finalized",
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(102),
                    Some("0x102"),
                    Some("0xtx102"),
                    Some(0),
                    CanonicalityState::Finalized,
                )
            },
            history_event(
                "history:observed",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(103),
                Some("0x103"),
                Some("0xtx103"),
                Some(0),
                CanonicalityState::Observed,
            ),
            history_event(
                "history:orphaned",
                Some(logical_name_id),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec!["history:finalized", "history:safe", "history:canonical"]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.consistency, "head");
    assert_eq!(payload.last_updated, "2023-11-14T22:15:02Z");
    assert_eq!(payload.verified_state, None);
    assert_eq!(payload.declared_state, json!({}));
    assert_eq!(
        payload.coverage,
        CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["normalized_events".to_owned()],
            enumeration_basis: "canonical normalized-event history for the requested both scope"
                .to_owned(),
            unsupported_reason: None,
        }
    );
    assert_eq!(
        payload
            .provenance
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("normalized_event_history")
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.provenance.get("manifest_versions"),
        Some(&json!([
            {
                "manifest_version": 7,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": manifest_id_v7
            },
            {
                "manifest_version": 8,
                "source_family": "ens_v1_registry_l1",
                "source_manifest_id": manifest_id_v8
            }
        ]))
    );
    assert_eq!(
        payload
            .provenance
            .get("raw_fact_refs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3)
    );
    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 102,
                "block_hash": "0x102",
                "timestamp": "2023-11-14T22:15:02Z"
            }
        })
    );

    let first_row = payload
        .data
        .first()
        .and_then(Value::as_object)
        .expect("first history row must be an object");
    assert_eq!(
        first_row.get("canonicality_state").and_then(Value::as_str),
        Some("finalized")
    );
    assert_eq!(
        first_row.get("chain_position"),
        Some(&json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 102,
            "block_hash": "0x102",
            "timestamp": "2023-11-14T22:15:02Z"
        }))
    );
    assert_eq!(
        first_row.get("provenance"),
        Some(&json!({
            "after": "history:finalized"
        }))
    );
    assert_eq!(
        first_row.get("coverage"),
        Some(&json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["normalized_events"],
            "enumeration_basis": "history:finalized",
            "unsupported_reason": null
        }))
    );

    let compact_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?view=compact&page_size=1")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact name history request failed")?;
    assert_eq!(compact_response.status(), StatusCode::OK);
    let compact_payload: Value = read_json(compact_response).await?;
    assert!(compact_payload.get("declared_state").is_none());
    assert!(compact_payload.get("provenance").is_none());
    assert!(compact_payload.get("coverage").is_none());
    assert_eq!(
        compact_payload
            .get("meta")
            .and_then(|meta| meta.get("support_status"))
        .and_then(Value::as_str),
        Some("supported")
    );
    let compact_row = compact_payload
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .expect("compact history row must be an object");
    assert_eq!(
        compact_row.get("type").and_then(Value::as_str),
        Some("HistoryEvent")
    );
    assert_eq!(
        compact_row.get("name").and_then(Value::as_str),
        Some("alice.eth")
    );
    assert_eq!(compact_row.get("block_number"), Some(&json!(102)));
    assert_eq!(
        compact_row.get("timestamp").and_then(Value::as_str),
        Some("2023-11-14T22:15:02Z")
    );
    assert_eq!(
        compact_row.get("transaction_hash").and_then(Value::as_str),
        Some("0xtx102")
    );
    assert!(compact_row.get("normalized_event_id").is_none());
    assert!(compact_row.get("raw_fact_ref").is_none());
    assert!(compact_row.get("provenance").is_none());
    assert!(compact_row.get("coverage").is_none());

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?page_size=1&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("name history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_honors_scope_query_parameter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa100);
    let other_resource_id = Uuid::from_u128(0xa101);
    let surface_binding_id = Uuid::from_u128(0xb100);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x200", None, 200, 1_700_000_200),
            raw_block(
                "ethereum-mainnet",
                "0x201",
                Some("0x200"),
                201,
                1_700_000_201,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x202",
                Some("0x201"),
                202,
                1_700_000_202,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x203",
                Some("0x202"),
                203,
                1_700_000_203,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x204",
                Some("0x203"),
                204,
                1_700_000_204,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-only",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(200),
                Some("0x200"),
                Some("0xtx200"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(201),
                Some("0x201"),
                Some("0xtx201"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(202),
                Some("0x202"),
                Some("0xtx202"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(203),
                Some("0x203"),
                Some("0xtx203"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some(logical_name_id),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(204),
                Some("0x204"),
                Some("0xtx204"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=surface&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=resource&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=both&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_resource_scope_preserves_rebound_resources() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let old_resource_id = Uuid::from_u128(0xa120);
    let current_resource_id = Uuid::from_u128(0xa121);

    bigname_storage::upsert_name_surfaces(&database.pool, &[name_surface(logical_name_id)]).await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[resource(old_resource_id), resource(current_resource_id)],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            SurfaceBinding {
                active_to: Some(timestamp(1_700_000_250)),
                ..surface_binding(
                    Uuid::from_u128(0xb120),
                    logical_name_id,
                    old_resource_id,
                    timestamp(1_700_000_200),
                )
            },
            surface_binding(
                Uuid::from_u128(0xb121),
                logical_name_id,
                current_resource_id,
                timestamp(1_700_000_251),
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x220", None, 220, 1_700_000_220),
            raw_block(
                "ethereum-mainnet",
                "0x221",
                Some("0x220"),
                221,
                1_700_000_221,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x222",
                Some("0x221"),
                222,
                1_700_000_222,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "resource-old",
                None,
                Some(old_resource_id),
                Some("ethereum-mainnet"),
                Some(220),
                Some("0x220"),
                Some("0xtx220"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-current",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(221),
                Some("0x221"),
                Some("0xtx221"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-anchor",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(222),
                Some("0x222"),
                Some("0xtx222"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=resource&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name resource-scope history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["resource-current", "resource-old"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/alice.eth?scope=both&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec!["surface-anchor", "resource-current", "resource-old"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_returns_not_found_when_anchor_is_missing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/missing.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_history_returns_not_found_for_unsupported_namespace() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/unknown/alice.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_returns_chain_position_desc_ordering() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa300);
    let surface_binding_id = Uuid::from_u128(0xb300);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
            raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
            raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "no-chain-position",
                Some(logical_name_id),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-lower-log",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(1),
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-higher-log",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(7),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-same-height",
                Some(logical_name_id),
                Some(resource_id),
                Some("base-mainnet"),
                Some(100),
                Some("0xb100"),
                Some("0xtx090"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-higher-height",
                Some(logical_name_id),
                Some(resource_id),
                Some("base-mainnet"),
                Some(101),
                Some("0xb101"),
                Some("0xtx101"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec![
            "base-higher-height",
            "base-same-height",
            "ethereum-higher-log",
            "ethereum-lower-log",
            "no-chain-position",
        ]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(
        payload.chain_positions,
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 101,
                "block_hash": "0xb101",
                "timestamp": "2023-11-14T22:20:01Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 100,
                "block_hash": "0xe100",
                "timestamp": "2023-11-14T22:20:00Z"
            }
        })
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?page_size=1&view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_honors_scope_query_parameter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa200);
    let other_resource_id = Uuid::from_u128(0xa201);
    let surface_binding_id = Uuid::from_u128(0xb200);

    database
        .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
        .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x300", None, 300, 1_700_000_300),
            raw_block(
                "ethereum-mainnet",
                "0x301",
                Some("0x300"),
                301,
                1_700_000_301,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x302",
                Some("0x301"),
                302,
                1_700_000_302,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x303",
                Some("0x302"),
                303,
                1_700_000_303,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x304",
                Some("0x303"),
                304,
                1_700_000_304,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-only",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(300),
                Some("0x300"),
                Some("0xtx300"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(301),
                Some("0x301"),
                Some("0xtx301"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(302),
                Some("0x302"),
                Some("0xtx302"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(303),
                Some("0x303"),
                Some("0xtx303"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some(logical_name_id),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(304),
                Some("0x304"),
                Some("0xtx304"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?scope=surface&view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface resource-history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/resources/{resource_id}?scope=resource&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource resource-history request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?scope=both&view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("combined resource-history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_surface_scope_preserves_multiple_bound_surfaces() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa230);
    let primary_logical_name_id = "ens:alice.eth";
    let alias_logical_name_id = "ens:alice-base.eth";

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            name_surface(primary_logical_name_id),
            name_surface(alias_logical_name_id),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            surface_binding(
                Uuid::from_u128(0xb230),
                primary_logical_name_id,
                resource_id,
                timestamp(1_700_000_300),
            ),
            surface_binding(
                Uuid::from_u128(0xb231),
                alias_logical_name_id,
                resource_id,
                timestamp(1_700_000_301),
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x330", None, 330, 1_700_000_330),
            raw_block(
                "ethereum-mainnet",
                "0x331",
                Some("0x330"),
                331,
                1_700_000_331,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x332",
                Some("0x331"),
                332,
                1_700_000_332,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "surface-primary",
                Some(primary_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(330),
                Some("0x330"),
                Some("0xtx330"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-alias",
                Some(alias_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(331),
                Some("0x331"),
                Some("0xtx331"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-anchor",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(332),
                Some("0x332"),
                Some("0xtx332"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?scope=surface&view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource surface-scope history request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec!["surface-alias", "surface-primary"]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?scope=both&view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource combined history request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec!["resource-anchor", "surface-alias", "surface-primary"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_history_composes_current_and_historical_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let current_resource_id = Uuid::from_u128(0xa240);
    let current_token_lineage_id = Uuid::from_u128(0xa241);
    let current_surface_binding_id = Uuid::from_u128(0xb240);
    let basenames_resource_id = Uuid::from_u128(0xa242);
    let basenames_surface_binding_id = Uuid::from_u128(0xb242);
    let historical_resource_id = Uuid::from_u128(0xa243);
    let historical_token_lineage_id = Uuid::from_u128(0xa244);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x540", None, 540, 1_700_000_540),
            raw_block(
                "ethereum-mainnet",
                "0x541",
                Some("0x540"),
                541,
                1_700_000_541,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x542",
                Some("0x541"),
                542,
                1_700_000_542,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x543",
                Some("0x542"),
                543,
                1_700_000_543,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x544",
                Some("0x543"),
                544,
                1_700_000_544,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x545",
                Some("0x544"),
                545,
                1_700_000_545,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x546",
                Some("0x545"),
                546,
                1_700_000_546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(current_token_lineage_id, "0x540", 540),
            address_name_token_lineage(historical_token_lineage_id, "0x541", 541),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                current_resource_id,
                Some(current_token_lineage_id),
                "0x540",
                540,
            ),
            address_name_resource(basenames_resource_id, None, "0x546", 546),
            address_name_resource(
                historical_resource_id,
                Some(historical_token_lineage_id),
                "0x541",
                541,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface("ens:current.eth", "current.eth", "node:current.eth", 540),
            collection_name_surface(
                "basenames:filtered.base.eth",
                "filtered.base.eth",
                "node:filtered.base.eth",
                546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            address_name_surface_binding(
                current_surface_binding_id,
                "ens:current.eth",
                current_resource_id,
                "0x540",
                540,
                1_717_173_540,
            ),
            address_name_surface_binding(
                basenames_surface_binding_id,
                "basenames:filtered.base.eth",
                basenames_resource_id,
                "0x546",
                546,
                1_717_173_546,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                address,
                "ens:current.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "current.eth",
                "current.eth",
                "node:current.eth",
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                540,
            ),
            address_name_current_row(
                address,
                "basenames:filtered.base.eth",
                bigname_storage::AddressNameRelation::Registrant,
                "filtered.base.eth",
                "filtered.base.eth",
                "node:filtered.base.eth",
                basenames_surface_binding_id,
                basenames_resource_id,
                None,
                546,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "current-surface",
                Some("ens:current.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(544),
                Some("0x544"),
                Some("0xtx544"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(545),
                Some("0x545"),
                Some("0xtx545"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-surface",
                Some("ens:historical.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(543),
                Some("0x543"),
                Some("0xtx543"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-resource",
                None,
                Some(historical_resource_id),
                Some("ethereum-mainnet"),
                Some(542),
                Some("0x542"),
                Some("0xtx542"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_history_event(AuthorityHistorySeed {
                event_identity: "historical-match",
                namespace: "ens",
                logical_name_id: "ens:historical.eth",
                resource_id: historical_resource_id,
                event_kind: "RegistrationGranted",
                block_number: 541,
                block_hash: "0x541",
                after_state: json!({
                    "registrant": "0x0000000000000000000000000000000000000ABC",
                }),
            }),
            history_event(
                "filtered-basenames",
                Some("basenames:filtered.base.eth"),
                Some(basenames_resource_id),
                Some("ethereum-mainnet"),
                Some(546),
                Some("0x546"),
                Some("0xtx546"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: HistoryResponse = read_json(response).await?;
    assert_eq!(
        history_event_identities(&payload),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-match",
        ]
    );
    assert_eq!(payload.page.sort, "chain_position_desc");
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(
        payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("address history first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=ens&relation=registrant&page_size=1&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "chain_position_desc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ensv2_history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
    block_hash: &str,
    after_state: Value,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let mut event = history_event(
        event_identity,
        logical_name_id,
        resource_id,
        Some("ethereum-sepolia"),
        Some(block_number),
        Some(block_hash),
        Some(&format!("0xensv2tx{block_number}")),
        Some(0),
        canonicality_state,
    );
    event.event_kind = event_kind.to_owned();
    event.source_family = "ens_v2_registry_l1".to_owned();
    event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    event.before_state = json!({});
    event.after_state = ensv2_history_after_state(event_identity, after_state);
    event
}

fn ensv2_history_after_state(event_identity: &str, mut after_state: Value) -> Value {
    let object = after_state
        .as_object_mut()
        .expect("ENSv2 history test after_state must be an object");
    object.insert(
        "provenance".to_owned(),
        json!({
            "after": event_identity,
        }),
    );
    object.insert(
        "coverage".to_owned(),
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["normalized_events"],
            "enumeration_basis": event_identity,
            "unsupported_reason": null,
        }),
    );
    after_state
}

#[tokio::test]
async fn get_ensv2_history_routes_read_back_canonical_rows_and_address_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registrant = "0x0000000000000000000000000000000000000b0b";
    let controller = "0x0000000000000000000000000000000000000c0c";
    let unrelated = "0x0000000000000000000000000000000000000dad";
    let current_logical_name_id = "ens:current-v2.eth";
    let historical_logical_name_id = "ens:historical-v2.eth";
    let pending_logical_name_id = "ens:pending-v2.eth";
    let controller_logical_name_id = "ens:controller-v2.eth";
    let observed_logical_name_id = "ens:observed-v2.eth";
    let unrelated_logical_name_id = "ens:unrelated-v2.eth";
    let current_resource_id = Uuid::from_u128(0xa260);
    let current_token_lineage_id = Uuid::from_u128(0xa261);
    let current_surface_binding_id = Uuid::from_u128(0xb260);
    let historical_resource_id = Uuid::from_u128(0xa262);
    let controller_resource_id = Uuid::from_u128(0xa263);
    let observed_resource_id = Uuid::from_u128(0xa264);
    let unrelated_resource_id = Uuid::from_u128(0xa265);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-sepolia", "0xe702", None, 702, 1_700_000_702),
            raw_block(
                "ethereum-sepolia",
                "0xe703",
                Some("0xe702"),
                703,
                1_700_000_703,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe704",
                Some("0xe703"),
                704,
                1_700_000_704,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe705",
                Some("0xe704"),
                705,
                1_700_000_705,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe706",
                Some("0xe705"),
                706,
                1_700_000_706,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe707",
                Some("0xe706"),
                707,
                1_700_000_707,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe708",
                Some("0xe707"),
                708,
                1_700_000_708,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe709",
                Some("0xe708"),
                709,
                1_700_000_709,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe710",
                Some("0xe709"),
                710,
                1_700_000_710,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe711",
                Some("0xe710"),
                711,
                1_700_000_711,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe712",
                Some("0xe711"),
                712,
                1_700_000_712,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe713",
                Some("0xe712"),
                713,
                1_700_000_713,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe714",
                Some("0xe713"),
                714,
                1_700_000_714,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe715",
                Some("0xe714"),
                715,
                1_700_000_715,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe716",
                Some("0xe715"),
                716,
                1_700_000_716,
            ),
            raw_block(
                "ethereum-sepolia",
                "0xe717",
                Some("0xe716"),
                717,
                1_700_000_717,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xe700".to_owned(),
            block_number: 700,
            ..address_name_token_lineage(current_token_lineage_id, "0xe700", 700)
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            Resource {
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xe701".to_owned(),
                block_number: 701,
                ..address_name_resource(
                    current_resource_id,
                    Some(current_token_lineage_id),
                    "0xe701",
                    701,
                )
            },
            Resource {
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xe713".to_owned(),
                block_number: 713,
                ..address_name_resource(historical_resource_id, None, "0xe713", 713)
            },
            Resource {
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xe708".to_owned(),
                block_number: 708,
                ..address_name_resource(controller_resource_id, None, "0xe708", 708)
            },
            Resource {
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xe705".to_owned(),
                block_number: 705,
                ..address_name_resource(observed_resource_id, None, "0xe705", 705)
            },
            Resource {
                chain_id: "ethereum-sepolia".to_owned(),
                block_hash: "0xe703".to_owned(),
                block_number: 703,
                ..address_name_resource(unrelated_resource_id, None, "0xe703", 703)
            },
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(
                current_logical_name_id,
                "current-v2.eth",
                "node:current-v2.eth",
                701,
            ),
            collection_name_surface(
                historical_logical_name_id,
                "historical-v2.eth",
                "node:historical-v2.eth",
                713,
            ),
            collection_name_surface(
                pending_logical_name_id,
                "pending-v2.eth",
                "node:pending-v2.eth",
                711,
            ),
            collection_name_surface(
                controller_logical_name_id,
                "controller-v2.eth",
                "node:controller-v2.eth",
                708,
            ),
            collection_name_surface(
                observed_logical_name_id,
                "observed-v2.eth",
                "node:observed-v2.eth",
                705,
            ),
            collection_name_surface(
                unrelated_logical_name_id,
                "unrelated-v2.eth",
                "node:unrelated-v2.eth",
                703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xe701".to_owned(),
            block_number: 701,
            ..address_name_surface_binding(
                current_surface_binding_id,
                current_logical_name_id,
                current_resource_id,
                "0xe701",
                701,
                1_717_173_701,
            )
        }],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[
            address_name_current_row(
                registrant,
                current_logical_name_id,
                bigname_storage::AddressNameRelation::Registrant,
                "current-v2.eth",
                "current-v2.eth",
                "node:current-v2.eth",
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                701,
            ),
            address_name_current_row(
                controller,
                current_logical_name_id,
                bigname_storage::AddressNameRelation::EffectiveController,
                "current-v2.eth",
                "current-v2.eth",
                "node:current-v2.eth",
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                701,
            ),
        ],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            ensv2_history_event(
                "ensv2-current-observed",
                Some(current_logical_name_id),
                Some(current_resource_id),
                "TokenResourceLinked",
                717,
                "0xe717",
                json!({}),
                CanonicalityState::Observed,
            ),
            ensv2_history_event(
                "ensv2-current-resource",
                None,
                Some(current_resource_id),
                "TokenResourceLinked",
                716,
                "0xe716",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-current-surface",
                Some(current_logical_name_id),
                None,
                "LabelRegistered",
                715,
                "0xe715",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-historical-surface",
                Some(historical_logical_name_id),
                None,
                "LabelRegistered",
                714,
                "0xe714",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-historical-resource",
                None,
                Some(historical_resource_id),
                "TokenResourceLinked",
                713,
                "0xe713",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-historical-grant",
                Some(historical_logical_name_id),
                Some(historical_resource_id),
                "RegistrationGranted",
                712,
                "0xe712",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                }),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-pending-surface",
                Some(pending_logical_name_id),
                None,
                "LabelRegistered",
                711,
                "0xe711",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-pending-grant",
                Some(pending_logical_name_id),
                None,
                "RegistrationGranted",
                710,
                "0xe710",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                }),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-controller-surface",
                Some(controller_logical_name_id),
                None,
                "LabelRegistered",
                709,
                "0xe709",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-controller-resource",
                None,
                Some(controller_resource_id),
                "TokenResourceLinked",
                708,
                "0xe708",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-controller-authority",
                Some(controller_logical_name_id),
                Some(controller_resource_id),
                "AuthorityTransferred",
                707,
                "0xe707",
                json!({
                    "owner": "0x0000000000000000000000000000000000000C0C",
                }),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-observed-anchor-leak-surface",
                Some(observed_logical_name_id),
                None,
                "LabelRegistered",
                706,
                "0xe706",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-observed-anchor-leak-resource",
                None,
                Some(observed_resource_id),
                "TokenResourceLinked",
                705,
                "0xe705",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-observed-grant",
                Some(observed_logical_name_id),
                Some(observed_resource_id),
                "RegistrationGranted",
                704,
                "0xe704",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                }),
                CanonicalityState::Observed,
            ),
            ensv2_history_event(
                "ensv2-unrelated-surface",
                Some(unrelated_logical_name_id),
                None,
                "LabelRegistered",
                703,
                "0xe703",
                json!({}),
                CanonicalityState::Canonical,
            ),
            ensv2_history_event(
                "ensv2-unrelated-grant",
                Some(unrelated_logical_name_id),
                Some(unrelated_resource_id),
                "RegistrationGranted",
                702,
                "0xe702",
                json!({
                    "registrant": unrelated,
                }),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let name_both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/current-v2.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 name history request failed")?;
    assert_eq!(name_both_response.status(), StatusCode::OK);
    let name_both_payload: HistoryResponse = read_json(name_both_response).await?;
    assert_eq!(
        history_event_identities(&name_both_payload),
        vec!["ensv2-current-resource", "ensv2-current-surface"]
    );
    assert_eq!(name_both_payload.page.sort, "chain_position_desc");
    assert_eq!(name_both_payload.declared_state, json!({}));
    assert_eq!(
        name_both_payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );
    assert_eq!(
        name_both_payload.chain_positions["ethereum-sepolia"]["block_number"],
        json!(716)
    );
    assert_eq!(
        name_both_payload.data[0]
            .get("source_family")
            .and_then(Value::as_str),
        Some("ens_v2_registry_l1")
    );
    assert_eq!(
        name_both_payload.data[0]
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("ens_v2_registry_resource_surface")
    );

    let name_surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/current-v2.eth?scope=surface&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 name surface-scope history request failed")?;
    assert_eq!(name_surface_response.status(), StatusCode::OK);
    let name_surface_payload: HistoryResponse = read_json(name_surface_response).await?;
    assert_eq!(
        history_event_identities(&name_surface_payload),
        vec!["ensv2-current-surface"]
    );

    let name_resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/current-v2.eth?scope=resource&view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 name resource-scope history request failed")?;
    assert_eq!(name_resource_response.status(), StatusCode::OK);
    let name_resource_payload: HistoryResponse = read_json(name_resource_response).await?;
    assert_eq!(
        history_event_identities(&name_resource_payload),
        vec!["ensv2-current-resource"]
    );

    let resource_both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{current_resource_id}?view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 resource history request failed")?;
    assert_eq!(resource_both_response.status(), StatusCode::OK);
    let resource_both_payload: HistoryResponse = read_json(resource_both_response).await?;
    assert_eq!(
        history_event_identities(&resource_both_payload),
        vec!["ensv2-current-resource", "ensv2-current-surface"]
    );

    let resource_surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/resources/{current_resource_id}?scope=surface&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 resource surface-scope history request failed")?;
    assert_eq!(resource_surface_response.status(), StatusCode::OK);
    let resource_surface_payload: HistoryResponse = read_json(resource_surface_response).await?;
    assert_eq!(
        history_event_identities(&resource_surface_payload),
        vec!["ensv2-current-surface"]
    );

    let resource_resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/resources/{current_resource_id}?scope=resource&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 resource resource-scope history request failed")?;
    assert_eq!(resource_resource_response.status(), StatusCode::OK);
    let resource_resource_payload: HistoryResponse = read_json(resource_resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_resource_payload),
        vec!["ensv2-current-resource"]
    );

    let address_surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&scope=surface&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address surface-scope history request failed")?;
    assert_eq!(address_surface_response.status(), StatusCode::OK);
    let address_surface_payload: HistoryResponse = read_json(address_surface_response).await?;
    assert_eq!(
        history_event_identities(&address_surface_payload),
        vec![
            "ensv2-current-surface",
            "ensv2-historical-surface",
            "ensv2-historical-grant",
            "ensv2-pending-surface",
            "ensv2-pending-grant",
        ]
    );

    let address_resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&scope=resource&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address resource-scope history request failed")?;
    assert_eq!(address_resource_response.status(), StatusCode::OK);
    let address_resource_payload: HistoryResponse = read_json(address_resource_response).await?;
    assert_eq!(
        history_event_identities(&address_resource_payload),
        vec![
            "ensv2-current-resource",
            "ensv2-historical-resource",
            "ensv2-historical-grant",
        ]
    );

    let address_both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address history request failed")?;
    assert_eq!(address_both_response.status(), StatusCode::OK);
    let address_both_payload: HistoryResponse = read_json(address_both_response).await?;
    assert_eq!(
        history_event_identities(&address_both_payload),
        vec![
            "ensv2-current-resource",
            "ensv2-current-surface",
            "ensv2-historical-surface",
            "ensv2-historical-resource",
            "ensv2-historical-grant",
            "ensv2-pending-surface",
            "ensv2-pending-grant",
        ]
    );
    assert_eq!(
        address_both_payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );

    let address_first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&page_size=2&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address first page request failed")?;
    assert_eq!(address_first_page_response.status(), StatusCode::OK);
    let address_first_page_payload: HistoryResponse =
        read_json(address_first_page_response).await?;
    let cursor = address_first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("ENSv2 address first page must include next_cursor");

    let address_second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&page_size=2&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address second page request failed")?;
    assert_eq!(address_second_page_response.status(), StatusCode::OK);
    let address_second_page_payload: HistoryResponse =
        read_json(address_second_page_response).await?;

    let address_replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=ens&relation=registrant&page_size=2&cursor={cursor}&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address replay page request failed")?;
    assert_eq!(address_replay_page_response.status(), StatusCode::OK);
    let address_replay_page_payload: HistoryResponse =
        read_json(address_replay_page_response).await?;

    assert_replay_stable_pagination(
        &address_both_payload.data,
        &address_both_payload.page,
        &address_first_page_payload.data,
        &address_first_page_payload.page,
        &address_second_page_payload.data,
        &address_second_page_payload.page,
        &address_replay_page_payload.data,
        &address_replay_page_payload.page,
        "chain_position_desc",
        50,
        2,
    );

    let controller_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{controller}?namespace=ens&relation=effective_controller&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 address relation-filtered history request failed")?;
    assert_eq!(controller_response.status(), StatusCode::OK);
    let controller_payload: HistoryResponse = read_json(controller_response).await?;
    assert_eq!(
        history_event_identities(&controller_payload),
        vec![
            "ensv2-current-resource",
            "ensv2-current-surface",
            "ensv2-controller-surface",
            "ensv2-controller-resource",
            "ensv2-controller-authority",
        ]
    );

    let missing_name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/ens/missing-v2.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 missing name history request failed")?;
    assert_eq!(missing_name_response.status(), StatusCode::NOT_FOUND);
    let missing_name_payload: ErrorResponse = read_json(missing_name_response).await?;
    assert_eq!(missing_name_payload.error.code, "not_found");
    assert_eq!(
        missing_name_payload.error.message,
        "name missing-v2.eth was not found in namespace ens"
    );

    let missing_resource_id = Uuid::from_u128(0xa2ff);
    let missing_resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{missing_resource_id}?view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 missing resource history request failed")?;
    assert_eq!(missing_resource_response.status(), StatusCode::NOT_FOUND);
    let missing_resource_payload: ErrorResponse = read_json(missing_resource_response).await?;
    assert_eq!(missing_resource_payload.error.code, "not_found");
    assert_eq!(
        missing_resource_payload.error.message,
        format!("resource {missing_resource_id} was not found")
    );

    let unsupported_name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/unknown/current-v2.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 unsupported namespace name history request failed")?;
    assert_eq!(unsupported_name_response.status(), StatusCode::NOT_FOUND);
    let unsupported_name_payload: ErrorResponse = read_json(unsupported_name_response).await?;
    assert_eq!(unsupported_name_payload.error.code, "not_found");
    assert_eq!(
        unsupported_name_payload.error.message,
        "namespace unknown is not supported"
    );

    let unsupported_address_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{registrant}?namespace=unknown&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 unsupported address namespace history request failed")?;
    assert_eq!(
        unsupported_address_response.status(),
        StatusCode::BAD_REQUEST
    );
    let unsupported_address_payload: ErrorResponse =
        read_json(unsupported_address_response).await?;
    assert_eq!(unsupported_address_payload.error.code, "invalid_input");
    assert_eq!(
        unsupported_address_payload.error.message,
        "namespace must be one of: ens, basenames"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_basenames_history_routes_read_back_canonical_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000b0b";
    let current_logical_name_id = "basenames:alice.base.eth";
    let historical_logical_name_id = "basenames:legacy.base.eth";
    let current_resource_id = Uuid::from_u128(0xa245);
    let current_surface_binding_id = Uuid::from_u128(0xb245);
    let historical_resource_id = Uuid::from_u128(0xa246);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xb641", None, 641, 1_700_000_641),
            raw_block("base-mainnet", "0xb642", Some("0xb641"), 642, 1_700_000_642),
            raw_block("base-mainnet", "0xb643", Some("0xb642"), 643, 1_700_000_643),
            raw_block("base-mainnet", "0xb644", Some("0xb643"), 644, 1_700_000_644),
            raw_block("base-mainnet", "0xb645", Some("0xb644"), 645, 1_700_000_645),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            Resource {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xb641".to_owned(),
                block_number: 641,
                ..address_name_resource(current_resource_id, None, "0xb641", 641)
            },
            Resource {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xb642".to_owned(),
                block_number: 642,
                ..address_name_resource(historical_resource_id, None, "0xb642", 642)
            },
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            collection_name_surface(
                current_logical_name_id,
                "alice.base.eth",
                "node:alice.base.eth",
                641,
            ),
            collection_name_surface(
                historical_logical_name_id,
                "legacy.base.eth",
                "node:legacy.base.eth",
                642,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xb641".to_owned(),
            block_number: 641,
            ..address_name_surface_binding(
                current_surface_binding_id,
                current_logical_name_id,
                current_resource_id,
                "0xb641",
                641,
                1_717_173_641,
            )
        }],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            current_logical_name_id,
            bigname_storage::AddressNameRelation::Registrant,
            "alice.base.eth",
            "alice.base.eth",
            "node:alice.base.eth",
            current_surface_binding_id,
            current_resource_id,
            None,
            641,
        )],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "current-surface",
                    Some(current_logical_name_id),
                    None,
                    Some("base-mainnet"),
                    Some(644),
                    Some("0xb644"),
                    Some("0xtx644"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "current-resource",
                    None,
                    Some(current_resource_id),
                    Some("base-mainnet"),
                    Some(645),
                    Some("0xb645"),
                    Some("0xtx645"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "historical-surface",
                    Some(historical_logical_name_id),
                    None,
                    Some("base-mainnet"),
                    Some(643),
                    Some("0xb643"),
                    Some("0xtx643"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "historical-resource",
                    None,
                    Some(historical_resource_id),
                    Some("base-mainnet"),
                    Some(642),
                    Some("0xb642"),
                    Some("0xtx642"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                chain_id: Some("base-mainnet".to_owned()),
                ..authority_history_event(AuthorityHistorySeed {
                    event_identity: "historical-match",
                    namespace: "basenames",
                    logical_name_id: historical_logical_name_id,
                    resource_id: historical_resource_id,
                    event_kind: "RegistrationGranted",
                    block_number: 641,
                    block_hash: "0xb641",
                    after_state: json!({
                        "registrant": "0x0000000000000000000000000000000000000B0B",
                    }),
                })
            },
        ],
    )
    .await?;

    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/history/names/basenames/alice.base.eth?view=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames name history request failed")?;
    assert_eq!(name_response.status(), StatusCode::OK);
    let name_payload: HistoryResponse = read_json(name_response).await?;
    assert_eq!(
        history_event_identities(&name_payload),
        vec!["current-resource", "current-surface"]
    );
    assert_eq!(name_payload.declared_state, json!({}));
    assert_eq!(
        name_payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );
    assert_eq!(
        name_payload
            .provenance
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("normalized_event_history")
    );
    assert_eq!(
        name_payload.chain_positions["base"]["chain_id"],
        json!("base-mainnet")
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{current_resource_id}?view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames resource history request failed")?;
    assert_eq!(resource_response.status(), StatusCode::OK);
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec!["current-resource", "current-surface"]
    );
    assert_eq!(
        resource_payload.chain_positions["base"]["chain_id"],
        json!("base-mainnet")
    );

    let address_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?namespace=basenames&relation=registrant&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames address history request failed")?;
    assert_eq!(address_response.status(), StatusCode::OK);
    let address_payload: HistoryResponse = read_json(address_response).await?;
    assert_eq!(
        history_event_identities(&address_payload),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-match",
        ]
    );
    assert_eq!(address_payload.declared_state, json!({}));
    assert_eq!(
        address_payload.coverage.enumeration_basis,
        "canonical normalized-event history for the requested both scope"
    );
    assert_eq!(
        address_payload
            .provenance
            .get("derivation_kind")
            .and_then(Value::as_str),
        Some("normalized_event_history")
    );
    assert_eq!(
        address_payload.chain_positions["base"]["chain_id"],
        json!("base-mainnet")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_address_history_honors_scope_and_relation_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000def";
    let current_resource_id = Uuid::from_u128(0xa250);
    let current_token_lineage_id = Uuid::from_u128(0xa251);
    let current_surface_binding_id = Uuid::from_u128(0xb250);
    let controller_resource_id = Uuid::from_u128(0xa252);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0x550", None, 550, 1_700_000_550),
            raw_block(
                "ethereum-mainnet",
                "0x551",
                Some("0x550"),
                551,
                1_700_000_551,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x552",
                Some("0x551"),
                552,
                1_700_000_552,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x553",
                Some("0x552"),
                553,
                1_700_000_553,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x554",
                Some("0x553"),
                554,
                1_700_000_554,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x555",
                Some("0x554"),
                555,
                1_700_000_555,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            current_token_lineage_id,
            "0x550",
            550,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(
                current_resource_id,
                Some(current_token_lineage_id),
                "0x550",
                550,
            ),
            address_name_resource(controller_resource_id, None, "0x551", 551),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:current-controller.eth",
            "current-controller.eth",
            "node:current-controller.eth",
            550,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            current_surface_binding_id,
            "ens:current-controller.eth",
            current_resource_id,
            "0x550",
            550,
            1_717_173_550,
        )],
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            address,
            "ens:current-controller.eth",
            bigname_storage::AddressNameRelation::EffectiveController,
            "current-controller.eth",
            "current-controller.eth",
            "node:current-controller.eth",
            current_surface_binding_id,
            current_resource_id,
            Some(current_token_lineage_id),
            550,
        )],
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            history_event(
                "current-controller-surface",
                Some("ens:current-controller.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(554),
                Some("0x554"),
                Some("0xtx554"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-controller-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(555),
                Some("0x555"),
                Some("0xtx555"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-controller-surface",
                Some("ens:historical-controller.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(553),
                Some("0x553"),
                Some("0xtx553"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-controller-resource",
                None,
                Some(controller_resource_id),
                Some("ethereum-mainnet"),
                Some(552),
                Some("0x552"),
                Some("0xtx552"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_history_event(AuthorityHistorySeed {
                event_identity: "historical-controller-match",
                namespace: "ens",
                logical_name_id: "ens:historical-controller.eth",
                resource_id: controller_resource_id,
                event_kind: "AuthorityTransferred",
                block_number: 551,
                block_hash: "0x551",
                after_state: json!({
                    "owner": "0x0000000000000000000000000000000000000DEF",
                }),
            }),
        ],
    )
    .await?;

    let surface_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=surface&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history surface-scope request failed")?;
    let surface_payload: HistoryResponse = read_json(surface_response).await?;
    assert_eq!(
        history_event_identities(&surface_payload),
        vec![
            "current-controller-surface",
            "historical-controller-surface",
            "historical-controller-match",
        ]
    );

    let resource_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=resource&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history resource-scope request failed")?;
    let resource_payload: HistoryResponse = read_json(resource_response).await?;
    assert_eq!(
        history_event_identities(&resource_payload),
        vec![
            "current-controller-resource",
            "historical-controller-resource",
            "historical-controller-match",
        ]
    );

    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/history/addresses/{address}?relation=effective_controller&scope=both&view=full"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("address history combined request failed")?;
    let both_payload: HistoryResponse = read_json(both_response).await?;
    assert_eq!(
        history_event_identities(&both_payload),
        vec![
            "current-controller-resource",
            "current-controller-surface",
            "historical-controller-surface",
            "historical-controller-resource",
            "historical-controller-match",
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_history_returns_not_found_when_anchor_is_missing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa999);

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/history/resources/{resource_id}?view=full"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource history request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        format!("resource {resource_id} was not found")
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_returns_declared_state_collection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa300);
    let filtered_subject = "0x0000000000000000000000000000000000000abc";
    let other_subject = "0x0000000000000000000000000000000000000def";

    bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)]).await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                filtered_subject,
                PermissionScope::Resource,
                7,
                41,
            ),
            permission_current_row(
                resource_id,
                filtered_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                8,
                42,
            ),
            permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 43),
        ],
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/resources/{resource_id}/permissions"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResourcePermissionsResponse = read_json(response).await?;
    assert_eq!(
        permission_subjects(&payload),
        vec![filtered_subject, filtered_subject, other_subject]
    );
    assert!(payload.verified_state.is_none());
    assert_eq!(payload.declared_state, json!({}));
    assert_eq!(payload.page.page_size, 50);
    assert_eq!(payload.page.sort, "subject_scope_asc");
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["permissions_current".to_owned()]
    );
    assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert!(payload.provenance.is_null());

    let resource_row = payload
        .data
        .iter()
        .find(|row| {
            row.get("scope")
                .and_then(|value| value.get("kind"))
                .and_then(Value::as_str)
                == Some("resource")
        })
        .expect("resource row");
    assert_eq!(
        resource_row.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        resource_row.get("scope"),
        Some(&json!({
            "kind": "resource",
            "detail": {},
        }))
    );
    assert_eq!(
        resource_row.get("effective_powers"),
        Some(&json!(["set_resolver", "set_records"]))
    );
    assert_eq!(resource_row.get("revocation_source"), Some(&Value::Null));

    let resolver_row = payload
        .data
        .iter()
        .find(|row| {
            row.get("scope")
                .and_then(|value| value.get("kind"))
                .and_then(Value::as_str)
                == Some("resolver")
        })
        .expect("resolver row");
    assert_eq!(
        resolver_row.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        resolver_row.get("subject"),
        Some(&Value::String(filtered_subject.to_owned()))
    );
    assert_eq!(
        resolver_row.get("scope"),
        Some(&json!({
            "kind": "resolver",
            "detail": {
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000aaa",
            },
        }))
    );
    assert_eq!(
        resolver_row.get("effective_powers"),
        Some(&json!(["set_resolver", "create_subnames"]))
    );
    assert_eq!(
        resolver_row.get("grant_source"),
        Some(&json!({
            "kind": "normalized_event",
            "manifest_version": 8,
        }))
    );
    assert_eq!(
        resolver_row.get("inheritance_path"),
        Some(&json!([
            {
                "kind": "resource_authority",
                "resource_id": resource_id,
            }
        ]))
    );
    assert_eq!(
        resolver_row.get("transfer_behavior"),
        Some(&json!({
            "kind": "resource_rebound",
        }))
    );

    let first_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions first page request failed")?;
    assert_eq!(first_page_response.status(), StatusCode::OK);
    let first_page_payload: ResourcePermissionsResponse = read_json(first_page_response).await?;
    let cursor = first_page_payload
        .page
        .next_cursor
        .clone()
        .expect("resource permissions first page must include next_cursor");

    let second_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions second page request failed")?;
    assert_eq!(second_page_response.status(), StatusCode::OK);
    let second_page_payload: ResourcePermissionsResponse = read_json(second_page_response).await?;

    let replay_page_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions replay page request failed")?;
    assert_eq!(replay_page_response.status(), StatusCode::OK);
    let replay_page_payload: ResourcePermissionsResponse = read_json(replay_page_response).await?;

    assert_replay_stable_pagination(
        &payload.data,
        &payload.page,
        &first_page_payload.data,
        &first_page_payload.page,
        &second_page_payload.data,
        &second_page_payload.page,
        &replay_page_payload.data,
        &replay_page_payload.page,
        "subject_scope_asc",
        50,
        1,
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_honors_subject_and_scope_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource_id = Uuid::from_u128(0xa301);
    let other_resource_id = Uuid::from_u128(0xa302);
    let shared_subject = "0x0000000000000000000000000000000000000abc";
    let other_subject = "0x0000000000000000000000000000000000000def";
    let resolver_scope_filter =
        "resolver:ethereum-mainnet:0x0000000000000000000000000000000000000bbb";

    bigname_storage::upsert_resources(
        &database.pool,
        &[resource(resource_id), resource(other_resource_id)],
    )
    .await?;
    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            permission_current_row(
                resource_id,
                shared_subject,
                PermissionScope::Resource,
                7,
                51,
            ),
            permission_current_row(
                resource_id,
                shared_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                },
                8,
                52,
            ),
            permission_current_row(
                resource_id,
                shared_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000ccc".to_owned(),
                },
                10,
                54,
            ),
            permission_current_row(
                resource_id,
                other_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                },
                9,
                53,
            ),
            permission_current_row(
                other_resource_id,
                shared_subject,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                },
                11,
                55,
            ),
        ],
    )
    .await?;

    let subject_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?subject={shared_subject}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions subject filter request failed")?;
    assert_eq!(subject_response.status(), StatusCode::OK);
    let subject_payload: ResourcePermissionsResponse = read_json(subject_response).await?;
    assert_eq!(
        permission_subjects(&subject_payload),
        vec![shared_subject, shared_subject, shared_subject]
    );
    assert!(subject_payload.data.iter().all(|row| {
        row.get("resource_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == resource_id.to_string())
    }));

    let scope_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?scope={resolver_scope_filter}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions scope filter request failed")?;
    assert_eq!(scope_response.status(), StatusCode::OK);
    let scope_payload: ResourcePermissionsResponse = read_json(scope_response).await?;
    assert_eq!(
        permission_subjects(&scope_payload),
        vec![shared_subject, other_subject]
    );
    assert!(scope_payload.data.iter().all(|row| {
        row.get("resource_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == resource_id.to_string())
    }));
    assert!(scope_payload.data.iter().all(|row| {
        row.get("scope")
            == Some(&json!({
                "kind": "resolver",
                "detail": {
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000bbb",
                },
            }))
    }));

    let combined_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/resources/{resource_id}/permissions?subject={shared_subject}&scope={resolver_scope_filter}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resource permissions subject and scope filter request failed")?;
    assert_eq!(combined_response.status(), StatusCode::OK);
    let combined_payload: ResourcePermissionsResponse = read_json(combined_response).await?;
    assert_eq!(combined_payload.data.len(), 1);
    assert_eq!(
        combined_payload.data[0].get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        combined_payload.data[0].get("subject"),
        Some(&Value::String(shared_subject.to_owned()))
    );
    assert_eq!(
        combined_payload.data[0].get("scope"),
        Some(&json!({
            "kind": "resolver",
            "detail": {
                "chain_id": "ethereum-mainnet",
                "resolver_address": "0x0000000000000000000000000000000000000bbb",
            },
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resource_permissions_rejects_invalid_resource_id() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resources/not-a-uuid/permissions")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid resource permissions request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(payload.error.message, "resource_id must be a UUID");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}
