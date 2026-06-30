const DIAG_EVENTS_ADDRESS: &str = "0x0000000000000000000000000000000000000dea";
const DIAG_EVENTS_LOGICAL_NAME_ID: &str = "ens:diag.eth";
const DIAG_EVENTS_NAME: &str = "diag.eth";
const DIAG_EVENTS_RESOURCE_ID: u128 = 0xd1a90000000000000000000000000001;
const DIAG_EVENTS_TOKEN_LINEAGE_ID: u128 = 0xd1a90000000000000000000000000002;
const DIAG_EVENTS_SURFACE_BINDING_ID: u128 = 0xd1a90000000000000000000000000003;

#[tokio::test]
async fn v2_get_diagnostic_events_returns_raw_rows_and_infers_namespace() -> Result<()> {
    let (database, payload) =
        v2_diag_events_payload("/v2/diagnostics/events?name=Diag.eth&page_size=10").await?;

    assert_eq!(payload["page"]["page_size"], json!(10));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 305,
            "block_hash": "0xname131",
            "timestamp": "2026-04-17T00:00:05Z"
        })
    );

    let data = payload["data"]
        .as_array()
        .expect("diagnostic events data must be an array");
    let event_kinds = diagnostic_event_kinds(data);
    // latest-only: at/finality never bounds the event set; only meta.as_of reflects the
    // snapshot. diag:future (block 306, above the as_of head 305) is still returned.
    assert_eq!(
        event_kinds,
        vec![
            "SurfaceBound",
            "SurfaceBound",
            "RecordChanged",
            "RegistrationGranted",
            "TokenRegenerated"
        ]
    );
    assert_eq!(data[0]["event_identity"], json!("diag:future"));
    assert_eq!(data[0]["chain_position"]["block_number"], json!(306));

    let surface_bound = &data[1];
    assert_eq!(surface_bound["event_identity"], json!("diag:surface-bound"));
    assert_eq!(surface_bound["namespace"], json!("ens"));
    assert_eq!(surface_bound["name"], json!(DIAG_EVENTS_NAME));
    assert_eq!(surface_bound["event_kind"], json!("SurfaceBound"));
    assert_eq!(
        surface_bound["source_family"],
        json!("ens_v1_registry_l1")
    );
    assert_eq!(surface_bound["source_manifest_id"], Value::Null);
    assert_eq!(
        surface_bound["chain_position"],
        json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 305,
            "block_hash": "0xdiag305",
            "timestamp": "2023-11-14T22:18:25Z"
        })
    );
    assert_eq!(
        surface_bound["raw_fact_ref"],
        json!({"kind": "raw_log", "event_identity": "diag:surface-bound"})
    );
    assert_eq!(surface_bound["derivation_kind"], json!("diag_test_derivation"));
    assert_eq!(surface_bound["canonicality_state"], json!("canonical"));
    assert_eq!(surface_bound["before_state"]["state"], json!("before"));
    assert_eq!(surface_bound["after_state"]["state"], json!("after"));
    assert_eq!(
        surface_bound["provenance"],
        json!({"event_identity": "diag:surface-bound", "route": "diagnostics"})
    );
    assert_eq!(
        surface_bound["coverage"],
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["normalized_events"],
            "enumeration_basis": "diag_events",
            "unsupported_reason": null
        })
    );
    assert!(surface_bound.get("type").is_none());
    assert!(surface_bound.get("logical_name_id").is_none());
    assert!(surface_bound.get("registration_id").is_none());
    assert!(surface_bound.get("resource_id").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_diagnostic_events_paginates_and_rejects_cursor_mismatch() -> Result<()> {
    let (database, first_page) = v2_diag_events_payload(
        "/v2/diagnostics/events?namespace=ens&name=diag.eth&page_size=1",
    )
    .await?;

    assert_eq!(first_page["data"].as_array().expect("data").len(), 1);
    assert_eq!(first_page["page"]["has_more"], json!(true));
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();

    let second_page = v2_diag_events_payload_for_database(
        &database,
        &format!("/v2/diagnostics/events?namespace=ens&name=diag.eth&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    assert_ne!(second_page["data"], first_page["data"]);

    let filter_mismatch = v2_diag_events_response_for_database(
        &database,
        &format!(
            "/v2/diagnostics/events?namespace=ens&name=diag.eth&from_block=303&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(filter_mismatch.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(filter_mismatch).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    let snapshot_mismatch = v2_diag_events_response_for_database(
        &database,
        &format!(
            "/v2/diagnostics/events?namespace=ens&name=diag.eth&at=2023-11-14T22:18:23Z&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(snapshot_mismatch.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(snapshot_mismatch).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_diagnostic_events_honors_namespace_snapshot_and_filters() -> Result<()> {
    let (database, ens_snapshot) = v2_diag_events_payload(
        "/v2/diagnostics/events?namespace=ens&name=diag.eth&at=2023-11-14T22:18:23Z&finality=latest&page_size=10",
    )
    .await?;
    assert_eq!(
        ens_snapshot["meta"]["as_of"]["1"],
        json!({
            "block_number": 303,
            "block_hash": "0xdiag303",
            "timestamp": "2023-11-14T22:18:23Z"
        })
    );
    // latest-only: `at` sets meta.as_of (block 303) but does NOT cut the event set — all
    // diag.eth events are returned regardless of the snapshot block.
    assert_eq!(
        diagnostic_event_kinds(ens_snapshot["data"].as_array().expect("ens snapshot data")),
        vec![
            "SurfaceBound",
            "SurfaceBound",
            "RecordChanged",
            "RegistrationGranted",
            "TokenRegenerated"
        ]
    );

    let basenames =
        v2_diag_events_payload_for_database(&database, "/v2/diagnostics/events?namespace=basenames")
            .await?;
    let data = basenames["data"]
        .as_array()
        .expect("basenames diagnostic events data");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["namespace"], json!("basenames"));
    assert_eq!(data[0]["event_identity"], json!("diag:basenames:registration"));
    assert_eq!(
        basenames["meta"]["as_of"]["8453"],
        json!({
            "block_number": 401,
            "block_hash": "0xbasediag401",
            "timestamp": "2023-11-14T22:20:01Z"
        })
    );

    let filtered = v2_diag_events_payload_for_database(
        &database,
        &format!(
            "/v2/diagnostics/events?namespace=ens&address={DIAG_EVENTS_ADDRESS}&relation=registrant&type=registration&from_block=303&to_block=303"
        ),
    )
    .await?;
    assert_eq!(
        diagnostic_event_kinds(filtered["data"].as_array().expect("filtered data")),
        vec!["RegistrationGranted"]
    );
    assert_eq!(
        filtered["data"][0]["registration_id"],
        json!(Uuid::from_u128(DIAG_EVENTS_RESOURCE_ID).to_string())
    );
    assert!(filtered["data"][0].get("resource_id").is_none());

    database.cleanup().await?;
    Ok(())
}

async fn v2_diag_events_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_diag_events_fixture(&database).await?;
    let payload = v2_diag_events_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_diag_events_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_diag_events_response_for_database(database, uri).await?;
    let status = response.status();
    if status != StatusCode::OK {
        let payload: Value = read_json(response).await?;
        panic!("expected {uri} to return 200, got {status}: {payload}");
    }
    read_json(response).await
}

async fn v2_diag_events_response_for_database(
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
        .with_context(|| format!("v2 diagnostic events request failed for {uri}"))
}

async fn seed_v2_diag_events_fixture(database: &TestDatabase) -> Result<()> {
    let resource_id = Uuid::from_u128(DIAG_EVENTS_RESOURCE_ID);
    let token_lineage_id = Uuid::from_u128(DIAG_EVENTS_TOKEN_LINEAGE_ID);
    let surface_binding_id = Uuid::from_u128(DIAG_EVENTS_SURFACE_BINDING_ID);

    seed_v2_history_name(
        database,
        DIAG_EVENTS_LOGICAL_NAME_ID,
        "Diag.eth",
        "node:diag.eth",
        305,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            DIAG_EVENTS_ADDRESS,
            DIAG_EVENTS_LOGICAL_NAME_ID,
            bigname_storage::AddressNameRelation::Registrant,
            "Diag.eth",
            DIAG_EVENTS_NAME,
            "node:diag.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            303,
        )],
    )
    .await?;

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xdiag302", None, 302, 1_700_000_302),
            raw_block("ethereum-mainnet", "0xdiag303", None, 303, 1_700_000_303),
            raw_block("ethereum-mainnet", "0xdiag304", None, 304, 1_700_000_304),
            raw_block("ethereum-mainnet", "0xdiag305", None, 305, 1_700_000_305),
            raw_block("ethereum-mainnet", "0xdiag306", None, 306, 1_700_000_306),
            raw_block("base-mainnet", "0xbasediag401", None, 401, 1_700_000_401),
        ],
    )
    .await?;
    bigname_storage::advance_chain_checkpoints(
        &database.pool,
        &bigname_storage::ChainCheckpointUpdate {
            chain_id: "base-mainnet".to_owned(),
            canonical: Some(bigname_storage::CheckpointBlockRef {
                block_hash: "0xbasediag401".to_owned(),
                block_number: 401,
            }),
            ..bigname_storage::ChainCheckpointUpdate::default()
        },
    )
    .await?;

    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            v2_diag_event(
                "diag:future",
                "ens",
                Some(DIAG_EVENTS_LOGICAL_NAME_ID),
                None,
                "SurfaceBound",
                "ens_v1_registry_l1",
                "ethereum-mainnet",
                306,
                "0xdiag306",
            ),
            v2_diag_event(
                "diag:surface-bound",
                "ens",
                Some(DIAG_EVENTS_LOGICAL_NAME_ID),
                None,
                "SurfaceBound",
                "ens_v1_registry_l1",
                "ethereum-mainnet",
                305,
                "0xdiag305",
            ),
            v2_diag_event(
                "diag:record",
                "ens",
                Some(DIAG_EVENTS_LOGICAL_NAME_ID),
                None,
                "RecordChanged",
                "ens_v1_public_resolver",
                "ethereum-mainnet",
                304,
                "0xdiag304",
            ),
            v2_diag_event(
                "diag:registration",
                "ens",
                None,
                Some(resource_id),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                "ethereum-mainnet",
                303,
                "0xdiag303",
            ),
            v2_diag_event(
                "diag:token-regenerated",
                "ens",
                None,
                Some(resource_id),
                "TokenRegenerated",
                "ens_v2_registry_l1",
                "ethereum-mainnet",
                302,
                "0xdiag302",
            ),
            v2_diag_event(
                "diag:basenames:registration",
                "basenames",
                None,
                None,
                "RegistrationGranted",
                "basenames_base_registrar",
                "base-mainnet",
                401,
                "0xbasediag401",
            ),
        ],
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn v2_diag_event(
    event_identity: &str,
    namespace: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    source_family: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        namespace: namespace.to_owned(),
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 17,
        source_manifest_id: None,
        derivation_kind: "diag_test_derivation".to_owned(),
        before_state: json!({"state": "before"}),
        after_state: json!({
            "state": "after",
            "provenance": {
                "event_identity": event_identity,
                "route": "diagnostics"
            },
            "coverage": {
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": "diag_events",
                "unsupported_reason": null
            }
        }),
        ..history_event(
            event_identity,
            logical_name_id,
            resource_id,
            Some(chain_id),
            Some(block_number),
            Some(block_hash),
            Some(&format!("0xtx{block_number}")),
            Some(0),
            CanonicalityState::Canonical,
        )
    }
}

fn diagnostic_event_kinds(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["event_kind"].as_str().expect("diagnostic event kind"))
        .collect()
}
