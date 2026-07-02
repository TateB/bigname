#[tokio::test]
async fn v2_get_primary_name_shapes_answers_for_source_selection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    database
        .insert_primary_name_current_claim_row(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("alice.eth"),
        )
        .await?;

    let omitted = v2_primary_name_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name"),
    )
    .await?;
    let indexed = v2_primary_name_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name?source=indexed"),
    )
    .await?;
    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"),
    )
    .await?;

    assert_eq!(
        omitted["data"],
        json!({
            "address": V2_PRIMARY_NAME_ADDRESS,
            "coin_type": "60",
            "namespace": "ens",
            "answers": [
                {
                    "source": "indexed",
                    "status": "ok",
                    "name": "alice.eth"
                },
                {
                    "source": "verified",
                    "status": "not_found"
                }
            ]
        })
    );
    assert!(omitted.get("page").is_none());
    assert_primary_name_snapshot_meta(&omitted);
    assert!(omitted["meta"].get("source").is_none());
    assert_no_banned_v1_spellings(&omitted);

    assert_eq!(
        indexed["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "name": "alice.eth"
            }
        ])
    );
    assert_primary_name_snapshot_meta(&indexed);
    assert_eq!(indexed["meta"]["source"], json!("indexed"));

    assert_eq!(
        verified["data"]["answers"],
        json!([
            {
                "source": "verified",
                "status": "not_found"
            }
        ])
    );
    assert_primary_name_snapshot_meta(&verified);
    assert_eq!(verified["meta"]["source"], json!("verified"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_no_claim_tuple_returns_in_band_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let response = v2_primary_name_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!({
            "address": V2_PRIMARY_NAME_ADDRESS,
            "coin_type": "60",
            "namespace": "ens",
            "answers": [
                {
                    "source": "indexed",
                    "status": "not_found"
                },
                {
                    "source": "verified",
                    "status": "not_found"
                }
            ]
        })
    );
    assert!(payload["data"].get("verification").is_none());
    assert_primary_name_snapshot_meta(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_surfaces_persisted_mismatch_in_verification() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000063);
    let verified_primary_name = json!({
        "status": "mismatch",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        },
        "failure_reason": "resolved_target_mismatch"
    });

    database
        .insert_primary_name_current_claim_row(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("alice.eth"),
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name.clone(),
        timestamp(1_717_172_463),
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name,
        timestamp(1_717_172_463),
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name"),
    )
    .await?;

    assert_eq!(
        payload["data"],
        json!({
            "address": V2_PRIMARY_NAME_ADDRESS,
            "coin_type": "60",
            "namespace": "ens",
            "answers": [
                {
                    "source": "indexed",
                    "status": "ok",
                    "name": "alice.eth"
                },
                {
                    "source": "verified",
                    "status": "mismatch",
                    "name": "alice.eth",
                    "failure_reason": "resolved_target_mismatch"
                }
            ],
            "verification": {
                "status": "mismatch",
                "name": "alice.eth",
                "failure_reason": "resolved_target_mismatch"
            }
        })
    );
    assert_no_banned_v1_spellings(&payload);
    assert_primary_name_snapshot_meta(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_primary_name_verified_meta_spans_base_and_ethereum() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    seed_v2_basenames_primary_name_snapshot_positions(&database).await?;
    seed_v2_basenames_primary_name_persisted_verified(&database, address).await?;
    assert_basenames_primary_execution_artifact_slots(&database, address, &["ethereum"]).await?;

    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=verified"
        ),
    )
    .await?;
    assert_primary_name_snapshot_meta_chain_ids(&verified, &["1", "8453"]);
    assert_primary_name_snapshot_token_slots(&verified, &["base", "ethereum"]);
    assert_eq!(
        verified["meta"]["as_of"]["8453"]["block_hash"],
        json!("0xprimary-base")
    );
    assert_eq!(
        verified["meta"]["as_of"]["1"]["block_hash"],
        json!("0xprimary-ethereum")
    );

    let indexed = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=indexed"
        ),
    )
    .await?;
    assert_primary_name_snapshot_meta_chain_ids(&indexed, &["8453"]);
    assert_primary_name_snapshot_token_slots(&indexed, &["base"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_primary_name_without_persisted_verified_stays_base_scoped() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bce";
    seed_v2_basenames_primary_name_base_snapshot_position(&database).await?;
    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
        )
        .await?;

    let verified = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}&source=verified"
        ),
    )
    .await?;
    assert_eq!(
        verified["data"]["answers"],
        json!([{
            "source": "verified",
            "status": "not_found"
        }])
    );
    assert_primary_name_snapshot_meta_chain_ids(&verified, &["8453"]);
    assert_primary_name_snapshot_token_slots(&verified, &["base"]);

    let omitted_source = v2_primary_name_payload_for_database(
        &database,
        &format!(
            "/v2/addresses/{address}/primary-name?namespace=basenames&coin_type={V2_BASENAMES_PRIMARY_COIN_TYPE}"
        ),
    )
    .await?;
    assert_eq!(
        omitted_source["data"]["answers"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "name": "alice.base.eth"
            },
            {
                "source": "verified",
                "status": "not_found"
            }
        ])
    );
    assert_primary_name_snapshot_meta_chain_ids(&omitted_source, &["8453"]);
    assert_primary_name_snapshot_token_slots(&omitted_source, &["base"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_requires_reason_for_verified_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000064);
    let verified_primary_name = json!({
        "status": "unsupported"
    });

    database
        .insert_primary_name_current_claim_row(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("alice.eth"),
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name.clone(),
        timestamp(1_717_172_464),
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name,
        timestamp(1_717_172_464),
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = v2_primary_name_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"),
    )
    .await?;

    assert_eq!(
        payload["data"]["answers"],
        json!([
            {
                "source": "verified",
                "status": "unsupported",
                "unsupported_reason": "unsupported_reason_missing"
            }
        ])
    );
    assert_eq!(
        payload["data"]["verification"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "unsupported_reason_missing"
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_rejects_pipeline_unsupported_reason() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000065);
    let verified_primary_name = json!({
        "status": "unsupported",
        "unsupported_reason": "primary_name_sidecar_missing"
    });

    database
        .insert_primary_name_current_claim_row(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            V2_PRIMARY_NAME_ADDRESS,
            "ens",
            "60",
            Some("alice.eth"),
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name.clone(),
        timestamp(1_717_172_465),
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        V2_PRIMARY_NAME_ADDRESS,
        "60",
        verified_primary_name,
        timestamp(1_717_172_465),
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = v2_primary_name_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name?source=verified"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("internal_error"));
    assert_eq!(
        payload["error"]["message"],
        json!("failed to map primary-name reason vocabulary")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_runs_on_demand_claim_and_verification_for_default_tuple() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(V2_ON_DEMAND_PRIMARY_NAME_ADDRESS),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/addresses/{V2_ON_DEMAND_PRIMARY_NAME_ADDRESS}/primary-name"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 on-demand primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!({
            "address": V2_ON_DEMAND_PRIMARY_NAME_ADDRESS,
            "coin_type": "60",
            "namespace": "ens",
            "answers": [
                {
                    "source": "indexed",
                    "status": "ok",
                    "name": "taytems.eth"
                },
                {
                    "source": "verified",
                    "status": "ok",
                    "name": "taytems.eth"
                }
            ],
            "verification": {
                "status": "ok",
                "name": "taytems.eth"
            }
        })
    );
    assert_primary_name_omits_snapshot_meta(&payload);
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_primary_name_rejects_malformed_address() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response =
        v2_primary_name_response_for_database(&database, "/v2/addresses/not-an-address/primary-name")
            .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(response).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

const V2_PRIMARY_NAME_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
const V2_ON_DEMAND_PRIMARY_NAME_ADDRESS: &str = "0x8e8db5ccef88cca9d624701db544989c996e3216";
const V2_BASENAMES_PRIMARY_COIN_TYPE: &str = "2147492101";

async fn v2_primary_name_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_primary_name_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_primary_name_response_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 primary-name request failed")
}

fn assert_primary_name_snapshot_meta(payload: &Value) {
    assert!(
        payload["meta"]["as_of"].is_object(),
        "primary-name response must include meta.as_of"
    );
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("primary-name response must include meta.as_of_token");
    assert!(!token.is_empty(), "meta.as_of_token must not be empty");
    assert!(
        token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')),
        "meta.as_of_token must be URL-safe"
    );
}

fn assert_primary_name_omits_snapshot_meta(payload: &Value) {
    assert!(
        payload["meta"].get("as_of").is_none(),
        "primary-name response must omit meta.as_of"
    );
    assert!(
        payload["meta"].get("as_of_token").is_none(),
        "primary-name response must omit meta.as_of_token"
    );
}

fn assert_primary_name_snapshot_meta_chain_ids(payload: &Value, expected_chain_ids: &[&str]) {
    assert_primary_name_snapshot_meta(payload);
    let actual = payload["meta"]["as_of"]
        .as_object()
        .expect("primary-name meta.as_of must be an object")
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_chain_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
}

fn assert_primary_name_snapshot_token_slots(payload: &Value, expected_slots: &[&str]) {
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("primary-name response must include meta.as_of_token");
    let bigname_storage::SnapshotAt::ResolvedPositions(chain_positions) =
        crate::v2::decode_at_token(token).expect("primary-name token must decode")
    else {
        panic!("primary-name token must contain resolved chain positions");
    };
    let actual = chain_positions
        .as_map()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_slots
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
}

async fn assert_basenames_primary_execution_artifact_slots(
    database: &TestDatabase,
    address: &str,
    expected_slots: &[&str],
) -> Result<()> {
    let request_key = format!("basenames:{address}:{V2_BASENAMES_PRIMARY_COIN_TYPE}");
    let requested_positions: Value = sqlx::query_scalar(
        r#"
        SELECT requested_chain_positions
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = 'basenames'
          AND request_key = $2
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(request_key)
    .fetch_one(&database.pool)
    .await?;
    let actual = requested_positions
        .as_array()
        .expect("primary-name requested chain positions must be an array")
        .iter()
        .filter_map(|position| {
            position
                .get("chain_id")
                .and_then(Value::as_str)
                .and_then(|chain_id| chain_id.strip_suffix("-mainnet"))
        })
        .collect::<std::collections::BTreeSet<_>>();
    let expected = expected_slots
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);
    Ok(())
}

async fn seed_v2_basenames_primary_name_snapshot_positions(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 84_530_001,
                "block_hash": "0xprimary-base",
                "timestamp": "2026-04-17T00:10:01Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_010,
                "block_hash": "0xprimary-ethereum",
                "timestamp": "2026-04-17T00:10:00Z"
            }
        }))
        .await
}

async fn seed_v2_basenames_primary_name_base_snapshot_position(
    database: &TestDatabase,
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 84_530_001,
                "block_hash": "0xprimary-base",
                "timestamp": "2026-04-17T00:10:01Z"
            }
        }))
        .await
}

async fn seed_v2_basenames_primary_name_persisted_verified(
    database: &TestDatabase,
    address: &str,
) -> Result<()> {
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004a);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            V2_BASENAMES_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
        )
        .await?;

    let finished_at = timestamp(1_717_172_410);
    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        V2_BASENAMES_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        V2_BASENAMES_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    Ok(())
}
