#[tokio::test]
async fn v2_get_history_returns_lean_product_rows_newest_first() -> Result<()> {
    let (database, payload) = v2_history_payload("/v2/names/History.eth/history?page_size=20").await?;

    assert_eq!(payload["page"]["page_size"], json!(20));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 80,
            "block_hash": "0xname50",
            "timestamp": "2026-04-17T00:00:20Z"
        })
    );

    let data = payload["data"]
        .as_array()
        .expect("history data must be an array");
    assert_eq!(
        history_types(data),
        vec![
            "renewal",
            "expiry",
            "release",
            "permission",
            "record",
            "authority",
            "resolver",
            "transfer",
            "registration",
            "authority",
        ]
    );
    assert_eq!(
        data.iter()
            .map(|row| row["block_number"].as_i64().expect("block number"))
            .collect::<Vec<_>>(),
        vec![110, 109, 108, 107, 106, 105, 104, 103, 102, 101]
    );
    assert_eq!(data[0]["name"], json!("history.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(data[0]["timestamp"], json!("2023-11-14T22:15:10Z"));
    assert_eq!(data[0]["transaction_hash"], json!("0xtx110"));
    assert_eq!(data[0]["log_index"], json!(0));
    assert_eq!(
        data[0]["registration_id"],
        json!(Uuid::from_u128(0x7100).to_string())
    );
    assert!(
        data.iter()
            .any(|row| row["type"] == json!("record") && row.get("registration_id").is_none()),
        "surface-only rows must omit registration_id"
    );
    assert!(
        data.iter().any(|row| {
            row["block_number"] == json!(105)
                && row["transaction_hash"] == json!("0xtx105")
                && row["type"] == json!("authority")
                && row.get("registration_id").is_none()
        }),
        "AuthorityEpochChanged must surface as an authority history row"
    );
    for row in data {
        assert!(row.get("data").is_none());
        assert!(row.get("before").is_none());
        assert!(row.get("after").is_none());
        assert!(row.get("event_kind").is_none());
        assert!(row.get("normalized_event_id").is_none());
        assert!(row.get("resource_id").is_none());
    }
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_filters_non_product_rows_and_advances_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v2/names/History.eth/history?page_size=1").await?;

    assert_eq!(first_page["data"], json!([]));
    assert_eq!(first_page["page"]["has_more"], json!(true));
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("filtered first page must still expose next cursor");

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/History.eth/history?page_size=1&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(history_types(second_page["data"].as_array().expect("data")), vec!["renewal"]);
    assert_ne!(second_page["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_paginates_with_anchor_bound_cursor() -> Result<()> {
    let (database, first_page) =
        v2_history_payload("/v2/names/history.eth/history?page_size=3").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/history.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    let first_hashes = history_transaction_hashes(&first_page);
    let second_hashes = history_transaction_hashes(&second_page);
    assert!(
        first_hashes
            .iter()
            .all(|hash| !second_hashes.contains(hash)),
        "history pages must not overlap"
    );
    assert_eq!(first_hashes, vec!["0xtx110", "0xtx109"]);
    assert_eq!(second_hashes, vec!["0xtx108", "0xtx107", "0xtx106"]);

    let replay = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/history.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(replay["data"], second_page["data"]);
    assert_eq!(replay["page"], second_page["page"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_rejects_cross_name_and_cross_scope_cursor_reuse() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_history_name(
        &database,
        "ens:other.eth",
        "Other.eth",
        "node:other.eth",
        81,
        Uuid::from_u128(0x7200),
        Uuid::from_u128(0x8200),
        Uuid::from_u128(0x9200),
    )
    .await?;

    let first_page =
        v2_history_payload_for_database(&database, "/v2/names/history.eth/history?page_size=3")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let cross_name = v2_history_response_for_database(
        &database,
        &format!("/v2/names/other.eth/history?page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_name.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_name).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let cross_scope = v2_history_response_for_database(
        &database,
        &format!("/v2/names/history.eth/history?scope=name&page_size=3&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_scope.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(cross_scope).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_scope_filters_name_registration_and_both() -> Result<()> {
    let (database, name_scope) =
        v2_history_payload("/v2/names/history.eth/history?scope=name&page_size=20").await?;
    let registration_scope = v2_history_payload_for_database(
        &database,
        "/v2/names/history.eth/history?scope=registration&page_size=20",
    )
    .await?;
    let both_scope = v2_history_payload_for_database(
        &database,
        "/v2/names/history.eth/history?scope=both&page_size=20",
    )
    .await?;

    assert_eq!(history_types(name_scope["data"].as_array().expect("data")), vec![
        "record",
        "authority",
        "resolver",
        "authority",
    ]);
    assert_eq!(
        history_types(registration_scope["data"].as_array().expect("data")),
        vec![
            "renewal",
            "expiry",
            "release",
            "permission",
            "transfer",
            "registration",
        ]
    );
    assert_eq!(both_scope["data"].as_array().expect("data").len(), 10);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_empty_and_missing_name_semantics() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_name(
        &database,
        "ens:quiet.eth",
        "Quiet.eth",
        "node:quiet.eth",
        80,
        Uuid::from_u128(0x7300),
        Uuid::from_u128(0x8300),
        Uuid::from_u128(0x9300),
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=120).await?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[v2_history_event(
            "quiet-surface-bound",
            Some("ens:quiet.eth"),
            None,
            "SurfaceBound",
            120,
        )],
    )
    .await?;

    let payload =
        v2_history_payload_for_database(&database, "/v2/names/quiet.eth/history").await?;
    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    let response = v2_history_response_for_database(&database, "/v2/names/missing.eth/history")
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_history_uses_sepolia_positioned_at_token_on_mixed_checkpoints() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_checkpoint_names(&database).await?;
    seed_v2_mixed_checkpoint_history(&database).await?;

    let at = v2_sepolia_snapshot_token();
    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}/history?at={at}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(
        history_types(payload["data"].as_array().expect("history data")),
        vec!["registration"]
    );

    database.cleanup().await?;
    Ok(())
}

async fn v2_history_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let payload = v2_history_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_history_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_history_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_history_response_for_database(
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
        .context("v2 history request failed")
}

async fn seed_v2_history_fixture(database: &TestDatabase) -> Result<()> {
    let logical_name_id = "ens:history.eth";
    let resource_id = Uuid::from_u128(0x7100);
    seed_v2_history_name(
        database,
        logical_name_id,
        "History.eth",
        "node:history.eth",
        80,
        resource_id,
        Uuid::from_u128(0x8100),
        Uuid::from_u128(0x9100),
    )
    .await?;
    seed_v2_history_blocks(database, 101..=111).await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            v2_history_event(
                "history-surface-bound",
                Some(logical_name_id),
                None,
                "SurfaceBound",
                111,
            ),
            v2_history_event(
                "history-renewal",
                None,
                Some(resource_id),
                "RegistrationRenewed",
                110,
            ),
            v2_history_event(
                "history-expiry",
                None,
                Some(resource_id),
                "ExpiryChanged",
                109,
            ),
            v2_history_event(
                "history-release",
                None,
                Some(resource_id),
                "RegistrationReleased",
                108,
            ),
            v2_history_event(
                "history-permission",
                None,
                Some(resource_id),
                "PermissionChanged",
                107,
            ),
            v2_history_event(
                "history-record",
                Some(logical_name_id),
                None,
                "RecordChanged",
                106,
            ),
            v2_history_event(
                "history-authority-epoch",
                Some(logical_name_id),
                None,
                "AuthorityEpochChanged",
                105,
            ),
            v2_history_event(
                "history-resolver",
                Some(logical_name_id),
                None,
                "ResolverChanged",
                104,
            ),
            v2_history_event(
                "history-transfer",
                None,
                Some(resource_id),
                "TokenControlTransferred",
                103,
            ),
            v2_history_event(
                "history-registration",
                None,
                Some(resource_id),
                "RegistrationGranted",
                102,
            ),
            v2_history_event(
                "history-authority",
                Some(logical_name_id),
                None,
                "AuthorityTransferred",
                101,
            ),
        ],
    )
    .await
    .context("failed to upsert v2 history fixture events")?;

    Ok(())
}

async fn seed_v2_mixed_checkpoint_history(database: &TestDatabase) -> Result<()> {
    let logical_name_id = format!("ens:{V2_SEPOLIA_SNAPSHOT_NAME}");
    let resource_id = Uuid::from_u128(0x7e20);
    let block_number = V2_SEPOLIA_SNAPSHOT_BLOCK + 1;
    let block_hash = "0xv2-sepolia-history-event";

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-sepolia",
            block_hash,
            Some(V2_SEPOLIA_SNAPSHOT_HASH),
            block_number,
            1_776_384_711,
        )],
    )
    .await?;

    let mut event = history_event(
        "v2-sepolia-snapshot-history-registration",
        None,
        Some(resource_id),
        Some("ethereum-sepolia"),
        Some(block_number),
        Some(block_hash),
        Some("0xv2sepoliahistorytx"),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.namespace = "ens".to_owned();
    event.logical_name_id = Some(logical_name_id);
    event.event_kind = "RegistrationGranted".to_owned();
    event.source_family = "ens_v2_registry_l1".to_owned();
    event.derivation_kind = "ens_v2_exact_name_profile".to_owned();
    event.after_state = json!({
        "authority_kind": "ens_v2_registry",
        "authority_key": "registry:ethereum-sepolia:sepolia-pin",
        "registrant": "0x00000000000000000000000000000000000000aa",
    });
    bigname_storage::upsert_normalized_events(&database.pool, &[event]).await?;

    Ok(())
}

async fn seed_v2_history_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    seed_v2_subnames_bound_child(
        database,
        logical_name_id,
        display_name,
        namehash,
        block_number,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000001"
            }
        }),
    )
    .await
}

async fn seed_v2_history_blocks(
    database: &TestDatabase,
    range: std::ops::RangeInclusive<i64>,
) -> Result<()> {
    let blocks = range
        .map(|block_number| {
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{block_number}"),
                None,
                block_number,
                1_700_000_000 + block_number,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::upsert_raw_blocks(&database.pool, &blocks).await?;
    Ok(())
}

fn v2_history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
) -> NormalizedEvent {
    let mut event = history_event(
        event_identity,
        logical_name_id,
        resource_id,
        Some("ethereum-mainnet"),
        Some(block_number),
        Some(&format!("0xhistory{block_number}")),
        Some(&format!("0xtx{block_number}")),
        Some(0),
        CanonicalityState::Canonical,
    );
    event.event_kind = event_kind.to_owned();
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.before_state = json!({});
    event.after_state = v2_history_after_state(event_kind);
    event
}

fn v2_history_after_state(event_kind: &str) -> Value {
    match event_kind {
        "RegistrationGranted" => json!({
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:history",
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expiry": 1_900_000_000_i64,
        }),
        "RegistrationRenewed" | "ExpiryChanged" => json!({
            "expiry": 1_950_000_000_i64,
        }),
        "RegistrationReleased" => json!({
            "released_at": 1_960_000_000_i64,
        }),
        "TokenControlTransferred" => json!({
            "to": "0x00000000000000000000000000000000000000bb",
        }),
        "AuthorityTransferred" => json!({
            "owner": "0x00000000000000000000000000000000000000cc",
        }),
        "AuthorityEpochChanged" => json!({
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:history",
            "registry_owner": "0x00000000000000000000000000000000000000cc",
        }),
        "ResolverChanged" => json!({
            "resolver": "0x0000000000000000000000000000000000000abc",
            "namehash": "node:history.eth",
        }),
        "RecordChanged" => json!({
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "value": "0x0000000000000000000000000000000000000def",
        }),
        "PermissionChanged" => json!({
            "subject": "0x00000000000000000000000000000000000000dd",
            "scope": {
                "kind": "resource"
            },
            "powers": ["resource_control"],
        }),
        "SurfaceBound" => json!({
            "binding_kind": "declared_registry_path",
        }),
        _ => json!({}),
    }
}

fn history_types(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["type"].as_str().expect("history row type"))
        .collect()
}

fn history_transaction_hashes(payload: &Value) -> Vec<&str> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| {
            row["transaction_hash"]
                .as_str()
                .expect("history row transaction_hash")
        })
        .collect()
}
