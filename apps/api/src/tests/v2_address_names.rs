use std::collections::BTreeSet;

#[tokio::test]
async fn v2_get_address_names_returns_record_rows_with_relations_and_primary_flag() -> Result<()> {
    let (database, payload) =
        v2_address_names_payload(&format!("/v2/addresses/{V2_ADDRESS}/names")).await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 105,
            "block_hash": "0xname69",
            "timestamp": "2026-04-17T00:00:45Z"
        })
    );

    let data = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert_eq!(
        names(data),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(data[0]["display_name"], json!("alpha.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(data[0]["namehash"], json!("node:alpha.eth"));
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert_eq!(data[0]["relations"], json!(["registrant", "owner"]));
    assert_eq!(data[0]["is_primary"], json!(true));
    assert_eq!(data[1]["relations"], json!(["manager"]));
    assert_eq!(data[1]["is_primary"], json!(false));
    assert!(data[0].get("resolver").is_none());
    assert!(data[0].get("addresses").is_none());
    assert!(data[0].get("text_records").is_none());
    assert!(data[0].get("content_hash").is_none());
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_filters_owner_relation_and_q_prefix() -> Result<()> {
    let (database, owner_payload) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?relation=owner"
    ))
    .await?;

    let owner_rows = owner_payload["data"]
        .as_array()
        .expect("owner data must be an array");
    assert_eq!(
        names(owner_rows),
        vec!["alpha.eth", "gamma.eth", "shared-one.eth", "shared-two.eth"]
    );
    assert_eq!(owner_rows[0]["relations"], json!(["owner"]));
    assert_eq!(owner_rows[1]["relations"], json!(["owner"]));

    let q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=ga"),
    )
    .await?;
    let q_rows = q_payload["data"].as_array().expect("q data must be an array");
    assert_eq!(names(q_rows), vec!["gamma.eth"]);

    let lowercase_q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=al"),
    )
    .await?;
    let uppercase_q_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=AL"),
    )
    .await?;
    let lowercase_q_rows = lowercase_q_payload["data"]
        .as_array()
        .expect("lowercase q data must be an array");
    let uppercase_q_rows = uppercase_q_payload["data"]
        .as_array()
        .expect("uppercase q data must be an array");
    assert_eq!(names(lowercase_q_rows), vec!["alpha.eth"]);
    assert_eq!(names(uppercase_q_rows), names(lowercase_q_rows));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_filters_relation_sets_and_any() -> Result<()> {
    let (database, set_payload) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?relation=registrant,manager"
    ))
    .await?;
    let any_payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?relation=any"),
    )
    .await?;

    let set_rows = set_payload["data"]
        .as_array()
        .expect("relation set data must be an array");
    assert_eq!(names(set_rows), vec!["alpha.eth", "beta.eth"]);
    assert_eq!(set_rows[0]["relations"], json!(["registrant"]));
    assert_eq!(set_rows[1]["relations"], json!(["manager"]));

    let any_rows = any_payload["data"]
        .as_array()
        .expect("relation any data must be an array");
    assert_eq!(
        names(any_rows),
        vec![
            "alpha.eth",
            "beta.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(any_rows[0]["relations"], json!(["registrant", "owner"]));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_non_success_primary_claim_does_not_mark_primary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::NotFound,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: None,
        }],
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| row["is_primary"] == json!(false)));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_scopes_primary_claim_by_row_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    seed_identity_name(
        &database,
        "basenames:alpha.eth",
        "alpha.eth",
        "alpha.eth",
        "node:basenames-alpha.eth",
        Uuid::from_u128(0xe100),
        Uuid::from_u128(0xe101),
        Uuid::from_u128(0xe102),
        V2_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        106,
    )
    .await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?q=alpha"),
    )
    .await?;
    let rows = payload["data"]
        .as_array()
        .expect("address names data must be an array");
    let ens_alpha = rows
        .iter()
        .find(|row| row["namespace"] == json!("ens") && row["name"] == json!("alpha.eth"))
        .expect("ens alpha row must be present");
    let basenames_alpha = rows
        .iter()
        .find(|row| row["namespace"] == json!("basenames") && row["name"] == json!("alpha.eth"))
        .expect("basenames alpha row must be present");

    assert_eq!(ens_alpha["is_primary"], json!(true));
    assert_eq!(basenames_alpha["is_primary"], json!(false));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_dedupe_name_vs_registration() -> Result<()> {
    let (database, dedupe_name) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?dedupe=name"
    ))
    .await?;
    let dedupe_registration = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?dedupe=registration"),
    )
    .await?;

    let name_rows = dedupe_name["data"]
        .as_array()
        .expect("dedupe=name data must be an array");
    let registration_rows = dedupe_registration["data"]
        .as_array()
        .expect("dedupe=registration data must be an array");

    assert_eq!(name_rows.len(), 5);
    assert_eq!(registration_rows.len(), 4);
    assert_eq!(
        name_rows
            .iter()
            .filter(|row| {
                row["name"] == json!("shared-one.eth") || row["name"] == json!("shared-two.eth")
            })
            .count(),
        2
    );
    assert_eq!(
        registration_rows
            .iter()
            .filter(|row| {
                row["name"] == json!("shared-one.eth") || row["name"] == json!("shared-two.eth")
            })
            .count(),
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_sorts_by_expiry_and_registered_at() -> Result<()> {
    let (database, expires_asc) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&order=asc"
    ))
    .await?;
    let expires_desc = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&order=desc"),
    )
    .await?;
    let registered = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=registered_at"),
    )
    .await?;

    assert_eq!(
        names(expires_asc["data"].as_array().expect("expires asc data")),
        vec![
            "beta.eth",
            "alpha.eth",
            "gamma.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );
    assert_eq!(
        names(expires_desc["data"].as_array().expect("expires desc data")),
        vec![
            "shared-one.eth",
            "shared-two.eth",
            "gamma.eth",
            "alpha.eth",
            "beta.eth"
        ]
    );
    assert_eq!(
        names(registered["data"].as_array().expect("registered data")),
        vec![
            "gamma.eth",
            "alpha.eth",
            "beta.eth",
            "shared-one.eth",
            "shared-two.eth"
        ]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_paginates_and_rejects_bound_cursor_reuse() -> Result<()> {
    let (database, first_page) = v2_address_names_payload(&format!(
        "/v2/addresses/{V2_ADDRESS}/names?page_size=2"
    ))
    .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();
    let second_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    let first_names = names(first_page["data"].as_array().expect("first page data"));
    let second_names = names(second_page["data"].as_array().expect("second page data"));
    assert_eq!(first_names, vec!["alpha.eth", "beta.eth"]);
    assert_eq!(second_names, vec!["gamma.eth", "shared-one.eth"]);
    assert!(first_names.iter().all(|name| !second_names.contains(name)));
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));

    let cross_address = v2_address_names_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_OTHER_ADDRESS}/names?page_size=2&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let cross_sort = v2_address_names_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=2&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(cross_sort.status(), StatusCode::BAD_REQUEST);

    let expires_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?sort=expires_at&page_size=1"),
    )
    .await?;
    let expires_cursor = expires_page["page"]["next_cursor"]
        .as_str()
        .expect("expires page must include a cursor");
    let cross_timestamp_sort = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?sort=registered_at&page_size=1&cursor={expires_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_timestamp_sort.status(), StatusCode::BAD_REQUEST);

    let relation_set_page = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?relation=manager,owner&page_size=1"),
    )
    .await?;
    let relation_set_cursor = relation_set_page["page"]["next_cursor"]
        .as_str()
        .expect("relation set page must include a cursor");
    let reordered_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?relation=owner,manager&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(reordered_relation_set.status(), StatusCode::OK);
    let changed_relation_set = v2_address_names_response_for_database(
        &database,
        &format!(
            "/v2/addresses/{V2_ADDRESS}/names?relation=owner&page_size=1&cursor={relation_set_cursor}"
        ),
    )
    .await?;
    assert_eq!(changed_relation_set.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_include_role_summary_groups_permissions_by_address() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let alpha = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.name == "alpha.eth")
        .expect("alpha address-name fixture must exist");
    let current_inventory = address_name_record_inventory_current_row(&alpha);
    let mut stale_inventory = current_inventory.clone();
    stale_inventory.record_version_boundary =
        address_name_record_inventory_boundary_with_pointer(&alpha, Some(9_999), Some("TextChanged"));
    stale_inventory.selectors = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        }
    ]);
    stale_inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc"
            }
        }
    ]);
    database
        .insert_record_inventory_current_row(current_inventory)
        .await?;
    database
        .insert_record_inventory_current_row(stale_inventory)
        .await?;
    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?include=role_summary&page_size=1"),
    )
    .await?;

    let row = &payload["data"]
        .as_array()
        .expect("role-summary data must be an array")[0];
    assert_eq!(row["name"], json!("alpha.eth"));
    assert_eq!(row["record_count"], json!(3));
    assert_eq!(
        row["role_summary"],
        json!([
            {
                "address": V2_PERMISSION_SUBJECT,
                "grants": [
                    {
                        "grant_scope": {
                            "kind": "registry",
                            "detail": {}
                        },
                        "powers": ["set_resolver", "create_subnames"]
                    },
                    {
                        "grant_scope": {
                            "kind": "registration",
                            "detail": {}
                        },
                        "powers": ["registration_control", "resolver_control"]
                    }
                ]
            },
            {
                "address": V2_PERMISSION_OTHER_SUBJECT,
                "grants": [
                    {
                        "grant_scope": {
                            "kind": "migration_derived",
                            "detail": {
                                "predecessor_registration_id": v2_address_names_predecessor_resource_id().to_string()
                            }
                        },
                        "powers": ["set_resolver", "set_records"]
                    },
                    {
                        "grant_scope": {
                            "kind": "record_manager",
                            "detail": {
                                "chain_id": 1,
                                "manager": "0x0000000000000000000000000000000000000bb1"
                            }
                        },
                        "powers": ["set_resolver", "create_subnames"]
                    },
                    {
                        "grant_scope": {
                            "kind": "resolver",
                            "detail": {
                                "resolver": {
                                    "chain_id": 1,
                                    "address": "0x0000000000000000000000000000000000000aaa"
                                }
                            }
                        },
                        "powers": ["set_resolver", "set_records"]
                    },
                    {
                        "grant_scope": {
                            "kind": "transport_derived",
                            "detail": {
                                "transport": "l1_to_l2"
                            }
                        },
                        "powers": ["set_resolver", "create_subnames"]
                    }
                ]
            }
        ])
    );
    assert!(row["role_summary"][0].get("subject").is_none());
    assert!(row["role_summary"][0]["grants"][0].get("effective_powers").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_rejects_bad_address_and_unknown_include() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let bad_address = v2_address_names_response_for_database(
        &database,
        "/v2/addresses/not-an-address/names",
    )
    .await?;
    assert_eq!(bad_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(bad_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let bad_include = v2_address_names_response_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names?include=counts"),
    )
    .await?;
    assert_eq!(bad_include.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(bad_include).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_address_names_empty_returns_200_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let payload = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/names"),
    )
    .await?;

    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

const V2_ADDRESS: &str = "0x0000000000000000000000000000000000000abc";
const V2_OTHER_ADDRESS: &str = "0x0000000000000000000000000000000000000def";
const V2_PERMISSION_SUBJECT: &str = "0x0000000000000000000000000000000000000c01";
const V2_PERMISSION_OTHER_SUBJECT: &str = "0x0000000000000000000000000000000000000c02";

async fn v2_address_names_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let payload = v2_address_names_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_address_names_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_address_names_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_address_names_response_for_database(
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
        .context("v2 address names request failed")
}

fn names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("row must include name"))
        .collect()
}

async fn seed_v2_address_names_fixture(database: &TestDatabase) -> Result<()> {
    let specs = v2_address_name_specs();
    seed_v2_address_name_storage(database, &specs).await?;
    seed_v2_address_name_current_rows(database, &specs).await?;
    seed_v2_address_name_relations(database, &specs).await?;
    seed_v2_address_name_permissions(database).await?;
    upsert_primary_name_current_snapshots(
        &database.pool,
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: V2_ADDRESS.to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            normalized_claim_name: Some("alpha.eth".to_owned()),
        }],
    )
    .await?;
    Ok(())
}

async fn seed_v2_address_name_storage(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let surfaces = specs
        .iter()
        .map(|spec| {
            collection_name_surface(
                spec.logical_name_id,
                spec.name,
                spec.namehash,
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_resources = BTreeSet::new();
    let resources = specs
        .iter()
        .filter(|spec| seen_resources.insert(spec.resource_id))
        .map(|spec| {
            address_name_resource(
                spec.resource_id,
                Some(spec.token_lineage_id),
                spec.block_hash,
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_token_lineages = BTreeSet::new();
    let token_lineages = specs
        .iter()
        .filter(|spec| seen_token_lineages.insert(spec.token_lineage_id))
        .map(|spec| {
            address_name_token_lineage(
                spec.token_lineage_id,
                spec.block_hash,
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let bindings = specs
        .iter()
        .map(|spec| {
            address_name_surface_binding(
                spec.surface_binding_id,
                spec.logical_name_id,
                spec.resource_id,
                spec.block_hash,
                spec.block_number,
                1_717_180_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut seen_raw_blocks = BTreeSet::new();
    let raw_blocks = specs
        .iter()
        .filter(|spec| seen_raw_blocks.insert((spec.block_hash, spec.block_number)))
        .map(|spec| {
            raw_block(
                "ethereum-mainnet",
                spec.block_hash,
                None,
                spec.block_number,
                1_717_180_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();

    bigname_storage::upsert_raw_blocks(&database.pool, &raw_blocks).await?;
    bigname_storage::upsert_name_surfaces(&database.pool, &surfaces).await?;
    bigname_storage::upsert_token_lineages(&database.pool, &token_lineages).await?;
    bigname_storage::upsert_resources(&database.pool, &resources).await?;
    bigname_storage::upsert_surface_bindings(&database.pool, &bindings).await?;
    Ok(())
}

async fn seed_v2_address_name_current_rows(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let mut inserted = BTreeSet::new();
    for spec in specs {
        if !inserted.insert(spec.logical_name_id) {
            continue;
        }
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
                        "registrant": spec.registrant,
                        "expiry": spec.expires_at
                    }
                }),
            ))
            .await?;
    }
    Ok(())
}

async fn seed_v2_address_name_relations(
    database: &TestDatabase,
    specs: &[V2AddressNameSpec],
) -> Result<()> {
    let mut rows = Vec::new();
    for spec in specs {
        for relation in spec.relations {
            rows.push(address_name_current_row(
                V2_ADDRESS,
                spec.logical_name_id,
                *relation,
                spec.name,
                spec.name,
                spec.namehash,
                spec.surface_binding_id,
                spec.resource_id,
                Some(spec.token_lineage_id),
                spec.block_number,
            ));
        }
    }

    bigname_storage::upsert_address_names_current_rows(&database.pool, &rows).await?;
    Ok(())
}

async fn seed_v2_address_name_permissions(database: &TestDatabase) -> Result<()> {
    let alpha_resource_id = Uuid::from_u128(0xa100);
    let mut resource_row = permission_current_row(
        alpha_resource_id,
        V2_PERMISSION_SUBJECT,
        PermissionScope::Resource,
        7,
        107,
    );
    resource_row.effective_powers = json!(["resource_control", "resolver_control"]);
    resource_row.grant_source = json!({
        "kind": "ens_v1_authority",
        "authority_kind": "registry_owner",
        "authority_key": "registry:ethereum-mainnet:alpha",
        "source_event_kind": "Transfer"
    });

    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            resource_row,
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_SUBJECT,
                PermissionScope::Registry,
                8,
                108,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                },
                9,
                109,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::RecordManager {
                    chain_id: "ethereum-mainnet".to_owned(),
                    manager_address: "0x0000000000000000000000000000000000000BB1".to_owned(),
                },
                10,
                110,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::MigrationDerived {
                    predecessor_resource_id: v2_address_names_predecessor_resource_id(),
                },
                11,
                111,
            ),
            permission_current_row(
                alpha_resource_id,
                V2_PERMISSION_OTHER_SUBJECT,
                PermissionScope::TransportDerived {
                    transport: "l1_to_l2".to_owned(),
                },
                12,
                112,
            ),
        ],
    )
    .await?;
    Ok(())
}

fn v2_address_names_predecessor_resource_id() -> Uuid {
    Uuid::from_u128(0xa300)
}

fn v2_address_name_specs() -> Vec<V2AddressNameSpec> {
    vec![
        V2AddressNameSpec {
            logical_name_id: "ens:alpha.eth",
            name: "alpha.eth",
            namehash: "node:alpha.eth",
            resource_id: Uuid::from_u128(0xa100),
            token_lineage_id: Uuid::from_u128(0xa101),
            surface_binding_id: Uuid::from_u128(0xa102),
            block_hash: "0xname65",
            block_number: 101,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[
                bigname_storage::AddressNameRelation::TokenHolder,
                bigname_storage::AddressNameRelation::Registrant,
            ],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:beta.eth",
            name: "beta.eth",
            namehash: "node:beta.eth",
            resource_id: Uuid::from_u128(0xb100),
            token_lineage_id: Uuid::from_u128(0xb101),
            surface_binding_id: Uuid::from_u128(0xb102),
            block_hash: "0xname66",
            block_number: 102,
            owner: "0x00000000000000000000000000000000000000b1",
            registrant: "0x00000000000000000000000000000000000000b2",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2026-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::EffectiveController],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:gamma.eth",
            name: "gamma.eth",
            namehash: "node:gamma.eth",
            resource_id: Uuid::from_u128(0xc100),
            token_lineage_id: Uuid::from_u128(0xc101),
            surface_binding_id: Uuid::from_u128(0xc102),
            block_hash: "0xname67",
            block_number: 103,
            owner: "0x00000000000000000000000000000000000000c1",
            registrant: "0x00000000000000000000000000000000000000c2",
            registered_at: "2023-12-02T00:00:00Z",
            created_at: "2023-12-01T00:00:00Z",
            expires_at: "2028-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-one.eth",
            name: "shared-one.eth",
            namehash: "node:shared-one.eth",
            resource_id: Uuid::from_u128(0xd100),
            token_lineage_id: Uuid::from_u128(0xd101),
            surface_binding_id: Uuid::from_u128(0xd102),
            block_hash: "0xname68",
            block_number: 104,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: "0x00000000000000000000000000000000000000d2",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2024-04-01T00:00:00Z",
            expires_at: "2029-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
        V2AddressNameSpec {
            logical_name_id: "ens:shared-two.eth",
            name: "shared-two.eth",
            namehash: "node:shared-two.eth",
            resource_id: Uuid::from_u128(0xd100),
            token_lineage_id: Uuid::from_u128(0xd101),
            surface_binding_id: Uuid::from_u128(0xd202),
            block_hash: "0xname69",
            block_number: 105,
            owner: "0x00000000000000000000000000000000000000d1",
            registrant: "0x00000000000000000000000000000000000000d2",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2024-04-01T00:00:00Z",
            expires_at: "2029-01-02T00:00:00Z",
            relations: &[bigname_storage::AddressNameRelation::TokenHolder],
        },
    ]
}

struct V2AddressNameSpec {
    logical_name_id: &'static str,
    name: &'static str,
    namehash: &'static str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    block_hash: &'static str,
    block_number: i64,
    owner: &'static str,
    registrant: &'static str,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
    relations: &'static [bigname_storage::AddressNameRelation],
}

fn address_name_record_inventory_current_row(
    spec: &V2AddressNameSpec,
) -> bigname_storage::RecordInventoryCurrentRow {
    let mut row = record_inventory_current_row(spec.logical_name_id, spec.resource_id);
    row.record_version_boundary = address_name_record_inventory_boundary_with_pointer(spec, None, None);
    row.chain_positions = json!({
        "ethereum-mainnet": address_name_record_inventory_chain_position(spec)
    });
    row
}

fn address_name_record_inventory_boundary_with_pointer(
    spec: &V2AddressNameSpec,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": spec.logical_name_id,
        "resource_id": spec.resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": address_name_record_inventory_chain_position(spec)
    })
}

fn address_name_record_inventory_chain_position(spec: &V2AddressNameSpec) -> Value {
    json!({
        "chain_id": "ethereum-mainnet",
        "block_number": spec.block_number,
        "block_hash": format!("0xname{:02x}", spec.block_number),
        "timestamp": format!("2026-04-17T00:00:{:02}Z", spec.block_number % 60)
    })
}
