const V2_RESOLVER_ADDRESS: &str = "0x0000000000000000000000000000000000000aaa";
const DIVERGENT_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const DIVERGENT_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const DIVERGENT_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const DIVERGENT_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[test]
fn v2_bound_names_cursor_payload_round_trips_storage_cursor() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");
    let payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);

    assert_eq!(payload.sort, "name_asc");
    assert_eq!(
        payload.filters,
        std::collections::BTreeMap::from([
            ("chain_id".to_owned(), "1".to_owned()),
            ("resolver".to_owned(), V2_RESOLVER_ADDRESS.to_owned()),
            ("namespace".to_owned(), "ens".to_owned()),
        ])
    );
    assert_eq!(
        crate::v2::bound_names_storage_cursor(&payload, &binding).expect("cursor must decode"),
        cursor
    );
}

#[test]
fn v2_bound_names_cursor_rejects_wrong_chain_resolver_sort_or_snapshot() {
    let cursor = v2_bound_names_cursor();
    let binding = v2_bound_names_cursor_binding(V2_RESOLVER_ADDRESS, "snapshot-1");

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.sort = "wrong".to_owned();
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload
        .filters
        .insert("chain_id".to_owned(), "8453".to_owned());
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.filters.insert(
        "resolver".to_owned(),
        "0x0000000000000000000000000000000000000bbb".to_owned(),
    );
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());

    let mut payload = crate::v2::bound_names_cursor_payload(&cursor, &binding);
    payload.snapshot = Some("snapshot-2".to_owned());
    assert!(crate::v2::bound_names_storage_cursor(&payload, &binding).is_err());
}

#[test]
fn v2_resolver_include_controls_overview_sections_and_rejects_unknown() {
    let include = crate::v2::resolver_overview_include(&["nodes".to_owned()])
        .expect("valid include must parse");
    let overview = crate::v2::build_resolver_overview(
        resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS),
        1,
        include,
        empty_bound_names(),
    );
    let value = serde_json::to_value(overview).expect("overview must serialize");

    assert!(value["nodes"].is_array());
    assert!(value.get("aliases").is_none());
    assert!(value.get("roles").is_none());
    assert!(value.get("events").is_none());

    let include = crate::v2::resolver_overview_include(&[]).expect("empty include defaults to all");
    let overview = crate::v2::build_resolver_overview(
        resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS),
        1,
        include,
        empty_bound_names(),
    );
    let value = serde_json::to_value(overview).expect("overview must serialize");
    assert!(value["nodes"].is_array());
    assert!(value["aliases"].is_array());
    assert!(value["roles"].is_array());
    assert_eq!(value["events"], Value::Null);

    let error = crate::v2::resolver_overview_include(&["records".to_owned()])
        .expect_err("unknown include must fail");
    assert_eq!(error.code(), crate::v2::ErrorCode::InvalidInput);
}

#[tokio::test]
async fn v2_get_resolver_returns_overview_with_nested_bound_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
    )
    .await?;

    assert!(first_page.get("page").is_none());
    assert_eq!(first_page["meta"]["as_of"]["1"]["block_number"], json!(102));
    assert_eq!(first_page["data"]["chain_id"], json!(1));
    assert_eq!(first_page["data"]["address"], json!(V2_RESOLVER_ADDRESS));
    assert_eq!(
        first_page["data"]["counts"],
        json!({
            "nodes": 2,
            "aliases": 1,
            "role_holders": 1,
            "events": 2,
        })
    );
    assert!(first_page["data"].get("aliases").is_none());
    assert!(first_page["data"].get("roles").is_none());
    assert!(first_page["data"].get("events").is_none());
    assert_eq!(first_page["data"]["nodes"][0]["namespace"], json!("ens"));
    assert_eq!(first_page["data"]["nodes"][0]["name"], json!("Alice.eth"));

    let bound_names = &first_page["data"]["bound_names"];
    assert_eq!(bound_names["page"]["cursor"], Value::Null);
    assert_eq!(bound_names["page"]["page_size"], json!(1));
    assert_eq!(bound_names["page"]["total_count"], Value::Null);
    assert_eq!(bound_names["page"]["has_more"], json!(true));
    let next_cursor = bound_names["page"]["next_cursor"]
        .as_str()
        .expect("first page must provide a nested cursor");
    assert_eq!(bound_names["data"][0]["name"], json!("alpha.eth"));
    assert_eq!(bound_names["data"][0]["display_name"], json!("alpha.eth"));
    assert_eq!(bound_names["data"][0]["namespace"], json!("ens"));
    assert_eq!(bound_names["data"][0]["namehash"], json!("node:alpha.eth"));
    assert_eq!(
        bound_names["data"][0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        bound_names["data"][0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(bound_names["data"][0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(bound_names["data"][0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(bound_names["data"][0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(
        bound_names["data"][0]["resolver"],
        json!({
            "chain_id": 1,
            "address": V2_RESOLVER_ADDRESS,
        })
    );

    let second_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(second_page["data"]["bound_names"]["data"][0]["name"], json!("beta.eth"));
    assert_eq!(second_page["data"]["bound_names"]["page"]["has_more"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_returns_empty_bound_names_when_overview_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["data"]["bound_names"]["data"], json!([]));
    assert_eq!(payload["data"]["bound_names"]["page"]["has_more"], json!(false));
    assert!(payload.get("page").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_uses_dictionary_owner_and_registrant_precedence_for_bound_names(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &[
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "base-mainnet",
            "ethereum-mainnet",
        ],
    )
    .await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=50"),
    )
    .await?;
    let rows = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    let row = rows
        .iter()
        .find(|row| row["name"] == json!("precedence.eth"))
        .expect("precedence row must be present");

    assert_eq!(row["owner"], json!(DIVERGENT_CONTROL_OWNER));
    assert_eq!(row["registrant"], json!(DIVERGENT_REGISTRATION_REGISTRANT));
    assert_eq!(row["registration_status"], json!("active"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_reports_unsupported_requested_sections_in_meta() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[unsupported_resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!(
            "/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes,aliases,roles,events"
        ),
    )
    .await?;

    assert_eq!(payload["data"]["nodes"], Value::Null);
    assert_eq!(
        payload["meta"]["unsupported_fields"],
        json!(["nodes", "aliases", "roles", "events"])
    );
    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));
    assert_eq!(
        payload["meta"]["unsupported_reason"],
        json!("resolver_family_pending")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_reports_narrowed_unsupported_sections_as_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[unsupported_resolver_current_row(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;

    assert_eq!(payload["data"]["nodes"], Value::Null);
    assert_eq!(payload["meta"]["unsupported_fields"], json!(["nodes"]));
    assert_eq!(payload["meta"]["completeness"], json!("unsupported"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_filters_bound_names_by_declared_resolver_chain() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet"],
    )
    .await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes"),
    )
    .await?;
    let rows = payload["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");

    assert_eq!(names(rows), vec!["alpha.eth"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_paginates_route_chain_rows_across_interleaved_chains() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet", "ethereum-mainnet"],
    )
    .await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let first_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
    )
    .await?;
    let first_rows = first_page["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");
    assert_eq!(names(first_rows), vec!["alpha.eth"]);
    let next_cursor = first_page["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .expect("first page must provide a nested cursor");

    let second_page = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    let second_rows = second_page["data"]["bound_names"]["data"]
        .as_array()
        .expect("bound_names data must be an array");

    assert_eq!(names(second_rows), vec!["gamma.eth"]);
    assert_eq!(
        first_rows
            .iter()
            .chain(second_rows.iter())
            .map(|row| row["name"].as_str().expect("row must include name"))
            .collect::<Vec<_>>(),
        vec!["alpha.eth", "gamma.eth"]
    );
    assert_eq!(
        second_page["data"]["bound_names"]["page"]["next_cursor"],
        Value::Null
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_does_not_advertise_wrong_chain_lookahead_as_more() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture_with_chains(
        &database,
        &["ethereum-mainnet", "base-mainnet"],
    )
    .await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;

    let payload = v2_resolver_payload_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes&page_size=1"),
    )
    .await?;

    assert_eq!(
        names(
            payload["data"]["bound_names"]["data"]
                .as_array()
                .expect("bound_names data must be an array")
        ),
        vec!["alpha.eth"]
    );
    assert_eq!(payload["data"]["bound_names"]["page"]["has_more"], json!(false));
    assert_eq!(payload["data"]["bound_names"]["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_missing_overview_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = v2_resolver_response_for_database(
        &database,
        &format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_resolver_rejects_malformed_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    for uri in [
        format!("/v2/resolvers/ethereum-mainnet/{V2_RESOLVER_ADDRESS}"),
        format!("/v2/resolvers/99999999/{V2_RESOLVER_ADDRESS}"),
        "/v2/resolvers/1/not-an-address".to_owned(),
        format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=records"),
    ] {
        let response = v2_resolver_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "invalid_input");
    }

    database.cleanup().await?;
    Ok(())
}

async fn v2_resolver_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_resolver_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_resolver_response_for_database(
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
        .context("v2 resolver request failed")
}

async fn seed_v2_resolver_bound_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_resolver_bound_names_fixture_with_chains(
        database,
        &["ethereum-mainnet", "ethereum-mainnet"],
    )
    .await
}

async fn seed_v2_resolver_bound_names_fixture_with_chains(
    database: &TestDatabase,
    resolver_chains: &[&str],
) -> Result<()> {
    let mut specs = v2_address_name_specs();
    specs.push(V2AddressNameSpec {
        logical_name_id: "ens:precedence.eth",
        name: "precedence.eth",
        namehash: "node:precedence.eth",
        resource_id: Uuid::from_u128(0xe100),
        token_lineage_id: Uuid::from_u128(0xe101),
        surface_binding_id: Uuid::from_u128(0xe102),
        block_hash: "0xname70",
        block_number: 106,
        owner: DIVERGENT_REGISTRY_OWNER,
        registrant: DIVERGENT_REGISTRATION_REGISTRANT,
        registered_at: "2024-07-03T00:00:00Z",
        created_at: "2023-07-03T00:00:00Z",
        expires_at: "2027-07-03T00:00:00Z",
        relations: &[],
    });
    seed_v2_address_name_storage(database, &specs).await?;

    for (spec, resolver_chain) in specs.iter().zip(resolver_chains.iter().copied()) {
        let control_owner = (spec.logical_name_id == "ens:precedence.eth")
            .then_some(DIVERGENT_CONTROL_OWNER);
        let control_registrant = if spec.logical_name_id == "ens:precedence.eth" {
            DIVERGENT_CONTROL_REGISTRANT
        } else {
            spec.registrant
        };

        database
            .insert_name_current_row(address_name_name_current_row(
                spec.logical_name_id,
                spec.name,
                spec.name,
                spec.namehash,
                spec.surface_binding_id,
                spec.resource_id,
                Some(spec.token_lineage_id),
                spec.block_number,
                json!({
                    "registration": {
                        "status": "active",
                        "authority_kind": "registrar",
                        "registrant": spec.registrant,
                        "registered_at": spec.registered_at,
                        "created_at": spec.created_at,
                        "expiry": spec.expires_at
                    },
                    "control": {
                        "registry_owner": spec.owner,
                        "owner": control_owner,
                        "registrant": control_registrant,
                        "expiry": spec.expires_at
                    },
                    "resolver": {
                        "chain_id": resolver_chain,
                        "address": V2_RESOLVER_ADDRESS,
                        "latest_event_kind": "ResolverChanged"
                    }
                }),
            ))
            .await?;
    }

    Ok(())
}

fn v2_bound_names_cursor() -> bigname_storage::NameCurrentListCursor {
    bigname_storage::NameCurrentListCursor {
        sort_value: bigname_storage::NameCurrentListCursorValue::Name("Alice.eth".to_owned()),
        namespace: "ens".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "node:alice.eth".to_owned(),
    }
}

fn v2_bound_names_cursor_binding<'a>(
    resolver_address: &'a str,
    snapshot_token: &'a str,
) -> crate::v2::BoundNamesCursorBinding<'a> {
    crate::v2::BoundNamesCursorBinding {
        chain_id: 1,
        resolver_address,
        namespace: Some("ens"),
        sort: "name_asc",
        snapshot_token,
    }
}

fn empty_bound_names() -> crate::v2::BoundNames {
    crate::v2::BoundNames {
        data: Vec::new(),
        page: crate::v2::Page {
            cursor: None,
            next_cursor: None,
            page_size: 50,
            total_count: None,
            has_more: false,
        },
    }
}

fn unsupported_resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
    let mut row = resolver_current_row(chain_id, resolver_address);
    row.declared_summary = json!({
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
    });
    row
}
