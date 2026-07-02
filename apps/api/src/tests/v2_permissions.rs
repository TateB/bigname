#[tokio::test]
async fn v2_get_permissions_requires_at_least_one_filter() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = v2_permissions_response_for_database(&database, "/v2/permissions").await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("at least one of name, registration_id, or address is required")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_rejects_conflicting_name_and_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let stale_resource_id = v2_permissions_stale_resource_id();

    let response = v2_permissions_response_for_database(
        &database,
        &format!("/v2/permissions?name=perms.eth&registration_id={stale_resource_id}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("unsupported"));
    assert_eq!(
        payload["error"]["message"],
        json!("conflicting registration filters")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_maps_rows_and_lineage() -> Result<()> {
    let (database, payload) = v2_permissions_payload(&format!(
        "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=10"
    ))
    .await?;
    let current_resource_id = v2_permissions_current_resource_id();
    let stale_resource_id = v2_permissions_stale_resource_id();

    assert_eq!(payload["page"]["page_size"], json!(10));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 130,
            "block_hash": "0xname82",
            "timestamp": "2026-04-17T00:00:10Z"
        })
    );

    let rows = payload["data"]
        .as_array()
        .expect("permissions data must be an array");
    assert_eq!(rows.len(), 5);
    let resolver = permission_row_by_scope_kind(rows, "resolver");
    let record_manager = permission_row_by_scope_kind(rows, "record_manager");
    let migration_derived = permission_row_by_scope_kind(rows, "migration_derived");
    let transport_derived = permission_row_by_scope_kind(rows, "transport_derived");
    let stale = permission_row_by_registration(rows, stale_resource_id);

    assert_eq!(resolver["address"], json!(V2_PERMISSIONS_SUBJECT));
    assert_eq!(
        resolver["registration_id"],
        json!(current_resource_id.to_string())
    );
    assert_eq!(resolver["name"], json!("perms.eth"));
    assert_eq!(
        resolver["grant_scope"],
        json!({
            "kind": "resolver",
            "detail": {
                "resolver": {
                    "chain_id": 1,
                    "address": "0x0000000000000000000000000000000000000abc"
                }
            }
        })
    );
    assert_eq!(
        resolver["powers"],
        json!(["set_resolver", "create_subnames"])
    );
    assert_eq!(
        resolver["lineage"],
        json!({
            "grant": {
                "kind": "event"
            },
            "revocation": {
                "kind": "event"
            },
            "inheritance_path": [
                {
                    "kind": "resolver_root_fallback",
                    "resolver": {
                        "chain_id": 1,
                        "address": "0x0000000000000000000000000000000000000abc"
                    }
                },
                {
                    "kind": "registry_root_fallback"
                }
            ]
        })
    );

    assert_eq!(
        record_manager["grant_scope"],
        json!({
            "kind": "record_manager",
            "detail": {
                "chain_id": 1,
                "manager": "0x0000000000000000000000000000000000000cc3"
            }
        })
    );
    assert_eq!(
        migration_derived["grant_scope"],
        json!({
            "kind": "migration_derived",
            "detail": {
                "predecessor_registration_id": v2_permissions_predecessor_resource_id().to_string()
            }
        })
    );
    assert_eq!(
        transport_derived["grant_scope"],
        json!({
            "kind": "transport_derived",
            "detail": {
                "transport": "l1_to_l2"
            }
        })
    );

    assert_eq!(stale["registration_id"], json!(stale_resource_id.to_string()));
    assert!(stale.get("name").is_none());
    assert_eq!(
        stale["grant_scope"],
        json!({
            "kind": "registration",
            "detail": {}
        })
    );
    assert_eq!(
        stale["powers"],
        json!(["registration_control", "resolver_control"])
    );
    assert_eq!(
        stale["lineage"],
        json!({
            "grant": {
                "kind": "ens_v1_authority"
            }
        })
    );
    assert!(stale["lineage"].get("revocation").is_none());
    assert!(stale["lineage"].get("inheritance_path").is_none());
    assert!(stale["lineage"].get("transfer_behavior").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_filters_by_name_registration_and_address() -> Result<()> {
    let (database, by_name) = v2_permissions_payload("/v2/permissions?name=Perms.eth").await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let name_rows = by_name["data"]
        .as_array()
        .expect("name-filtered permissions data");
    assert_eq!(name_rows.len(), 5);
    assert!(
        name_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );

    let by_registration = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?registration_id={current_resource_id}"),
    )
    .await?;
    let registration_rows = by_registration["data"]
        .as_array()
        .expect("registration-filtered permissions data");
    assert_eq!(registration_rows.len(), 5);
    assert!(
        registration_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );

    let by_address_and_registration = v2_permissions_payload_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&registration_id={current_resource_id}"
        ),
    )
    .await?;
    assert_eq!(
        by_address_and_registration["data"][0]["address"],
        json!(V2_PERMISSIONS_OTHER_SUBJECT)
    );
    assert_eq!(by_address_and_registration["data"].as_array().unwrap().len(), 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_name_filter_resolves_at_selected_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    seed_v2_permissions_sepolia_checkpoint(&database).await?;
    let current_resource_id = v2_permissions_current_resource_id();

    let no_at = v2_permissions_payload_for_database(&database, "/v2/permissions?name=Perms.eth")
        .await?;
    let no_at_rows = no_at["data"]
        .as_array()
        .expect("no-at name-filtered permissions data");
    assert_eq!(no_at_rows.len(), 5);
    assert!(
        no_at_rows
            .iter()
            .all(|row| row["registration_id"] == json!(current_resource_id.to_string()))
    );
    assert_eq!(
        no_at["meta"]["as_of"]["1"],
        json!({
            "block_number": 130,
            "block_hash": "0xname82",
            "timestamp": "2026-04-17T00:00:10Z"
        })
    );
    assert!(no_at["meta"]["as_of"].get("11155111").is_none());

    let sepolia_at = v2_permissions_sepolia_snapshot_token()?;
    let sepolia = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?name=Perms.eth&at={sepolia_at}"),
    )
    .await?;
    assert_eq!(sepolia["data"], json!([]));
    assert_eq!(sepolia["page"]["has_more"], json!(false));
    assert_eq!(sepolia["page"]["next_cursor"], Value::Null);
    assert_eq!(
        sepolia["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_PERMISSIONS_SEPOLIA_BLOCK,
            "block_hash": V2_PERMISSIONS_SEPOLIA_HASH,
            "timestamp": V2_PERMISSIONS_SEPOLIA_TIMESTAMP
        })
    );
    assert!(sepolia["meta"]["as_of"].get("1").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_name_filter_preserves_same_profile_stale() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    seed_v2_permissions_mainnet_rewind_checkpoint(&database).await?;

    let stale_at = v2_permissions_mainnet_rewind_snapshot_token()?;
    let response = v2_permissions_response_for_database(
        &database,
        &format!("/v2/permissions?name=Perms.eth&at={stale_at}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        payload["error"]["message"],
        json!("requested snapshot is not available for permissions")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_paginates_and_rejects_mismatched_cursor() -> Result<()> {
    let (database, first_page) = v2_permissions_payload(&format!(
        "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1"
    ))
    .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();

    let second_page = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&page_size=1&cursor={next_cursor}"),
    )
    .await?;
    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["has_more"], json!(true));
    assert_ne!(first_page["data"], second_page["data"]);

    let cross_address = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_OTHER_SUBJECT}&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_address).await?["error"]["code"],
        json!("invalid_input")
    );

    let cross_include = v2_permissions_response_for_database(
        &database,
        &format!(
            "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=1&cursor={next_cursor}"
        ),
    )
    .await?;
    assert_eq!(cross_include.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(cross_include).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_permissions_empty_results_return_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let by_address = v2_permissions_payload_for_database(
        &database,
        &format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}"),
    )
    .await?;
    assert_eq!(by_address["data"], json!([]));
    assert_eq!(by_address["page"]["has_more"], json!(false));
    assert_eq!(by_address["page"]["next_cursor"], Value::Null);

    let by_missing_name =
        v2_permissions_payload_for_database(&database, "/v2/permissions?name=missing.eth").await?;
    assert_eq!(by_missing_name["data"], json!([]));
    assert_eq!(by_missing_name["page"]["has_more"], json!(false));
    assert_eq!(by_missing_name["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

const V2_PERMISSIONS_SUBJECT: &str = "0x0000000000000000000000000000000000000cc1";
const V2_PERMISSIONS_OTHER_SUBJECT: &str = "0x0000000000000000000000000000000000000cc2";
const V2_PERMISSIONS_MAINNET_REWIND_BLOCK: i64 = 129;
const V2_PERMISSIONS_MAINNET_REWIND_HASH: &str = "0xv2-permissions-mainnet-rewind";
const V2_PERMISSIONS_MAINNET_REWIND_TIMESTAMP: &str = "2026-04-17T00:00:09Z";
const V2_PERMISSIONS_SEPOLIA_BLOCK: i64 = 111_551_130;
const V2_PERMISSIONS_SEPOLIA_HASH: &str = "0xv2-permissions-sepolia";
const V2_PERMISSIONS_SEPOLIA_TIMESTAMP: &str = "2026-04-17T00:20:30Z";

async fn v2_permissions_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    let payload = v2_permissions_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_permissions_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = v2_permissions_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_permissions_response_for_database(
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
        .context("v2 permissions request failed")
}

async fn seed_v2_permissions_fixture(database: &TestDatabase) -> Result<()> {
    let current_resource_id = v2_permissions_current_resource_id();
    let stale_resource_id = v2_permissions_stale_resource_id();
    let token_lineage_id = Uuid::from_u128(0xe102);
    let surface_binding_id = Uuid::from_u128(0xe103);

    database
        .seed_name_current_binding_migrated(
            "ens:perms.eth",
            current_resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(address_name_name_current_row(
            "ens:perms.eth",
            "Perms.eth",
            "perms.eth",
            "node:perms.eth",
            surface_binding_id,
            current_resource_id,
            Some(token_lineage_id),
            130,
            json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "control": {
                    "registry_owner": V2_PERMISSIONS_SUBJECT
                }
            }),
        ))
        .await?;
    bigname_storage::upsert_resources(&database.pool, &[resource(stale_resource_id)]).await?;

    let mut current_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000ABC".to_owned(),
        },
        8,
        108,
    );
    current_row.grant_source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": ["set_resolver"],
        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c108"
    });
    current_row.revocation_source = Some(json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": ["set_resolver"],
        "resolver_contract_instance_id": "00000000-0000-0000-0000-00000000c109"
    }));
    current_row.inheritance_path = json!([
        {
            "kind": "resolver_root_fallback",
            "chain_id": "ethereum-mainnet",
            "resolver_address": "0x0000000000000000000000000000000000000ABC",
            "upstream_resource": "root"
        },
        {
            "kind": "registry_root_fallback",
            "chain_id": "ethereum-mainnet",
            "registry_address": "0x0000000000000000000000000000000000000DEF",
            "upstream_resource": "root"
        }
    ]);
    current_row.transfer_behavior = json!({});

    let mut stale_row = permission_current_row(
        stale_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::Resource,
        7,
        109,
    );
    stale_row.effective_powers = json!(["resource_control", "resolver_control"]);
    stale_row.grant_source = json!({
        "kind": "ens_v1_authority",
        "authority_kind": "registry_owner",
        "authority_key": "registry:ethereum-mainnet:perms",
        "source_event_kind": "Transfer"
    });
    stale_row.inheritance_path = json!([]);
    stale_row.transfer_behavior = Value::Null;

    let mut record_manager_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::RecordManager {
            chain_id: "ethereum-mainnet".to_owned(),
            manager_address: "0x0000000000000000000000000000000000000cC3".to_owned(),
        },
        10,
        111,
    );
    apply_raw_log_permission_lineage(&mut record_manager_row, "set_records", 111);
    let mut migration_derived_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::MigrationDerived {
            predecessor_resource_id: v2_permissions_predecessor_resource_id(),
        },
        11,
        112,
    );
    apply_raw_log_permission_lineage(&mut migration_derived_row, "set_records", 112);
    let mut transport_derived_row = permission_current_row(
        current_resource_id,
        V2_PERMISSIONS_SUBJECT,
        PermissionScope::TransportDerived {
            transport: "l1_to_l2".to_owned(),
        },
        12,
        113,
    );
    apply_raw_log_permission_lineage(&mut transport_derived_row, "set_resolver", 113);

    bigname_storage::upsert_permissions_current_rows(
        &database.pool,
        &[
            current_row,
            record_manager_row,
            migration_derived_row,
            transport_derived_row,
            stale_row,
            permission_current_row(
                current_resource_id,
                V2_PERMISSIONS_OTHER_SUBJECT,
                PermissionScope::Registry,
                9,
                110,
            ),
        ],
    )
    .await?;

    Ok(())
}

async fn seed_v2_permissions_sepolia_checkpoint(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": V2_PERMISSIONS_SEPOLIA_BLOCK,
                "block_hash": V2_PERMISSIONS_SEPOLIA_HASH,
                "timestamp": V2_PERMISSIONS_SEPOLIA_TIMESTAMP
            }
        }))
        .await
}

async fn seed_v2_permissions_mainnet_rewind_checkpoint(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": V2_PERMISSIONS_MAINNET_REWIND_BLOCK,
                "block_hash": V2_PERMISSIONS_MAINNET_REWIND_HASH,
                "timestamp": V2_PERMISSIONS_MAINNET_REWIND_TIMESTAMP
            }
        }))
        .await
}

fn v2_permissions_mainnet_rewind_snapshot_token() -> Result<String> {
    v2_permissions_at_token(
        "ethereum",
        "ethereum-mainnet",
        V2_PERMISSIONS_MAINNET_REWIND_BLOCK,
        V2_PERMISSIONS_MAINNET_REWIND_HASH,
        V2_PERMISSIONS_MAINNET_REWIND_TIMESTAMP,
    )
}

fn v2_permissions_sepolia_snapshot_token() -> Result<String> {
    v2_permissions_at_token(
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_PERMISSIONS_SEPOLIA_BLOCK,
        V2_PERMISSIONS_SEPOLIA_HASH,
        V2_PERMISSIONS_SEPOLIA_TIMESTAMP,
    )
}

fn v2_permissions_at_token(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<String> {
    let position = bigname_storage::ChainPosition {
        slot: slot.to_owned(),
        chain_id: chain_id.to_owned(),
        block_number,
        block_hash: block_hash.to_owned(),
        timestamp: bigname_storage::parse_rfc3339_utc_timestamp(timestamp)
            .map_err(|error| anyhow::anyhow!("{error}"))?,
    };
    let selected = bigname_storage::SelectedSnapshot {
        chain_positions: bigname_storage::ChainPositions::new(std::collections::BTreeMap::from([
            (slot.to_owned(), position),
        ])),
        consistency: bigname_storage::SnapshotConsistency::Head,
    };
    Ok(crate::v2::encode_at_token(&selected))
}

fn apply_raw_log_permission_lineage(row: &mut bigname_storage::PermissionsCurrentRow, power: &str, suffix: i64) {
    row.grant_source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": "root",
        "root_resource": true,
        "changed_powers": [power],
        "resolver_contract_instance_id": format!("00000000-0000-0000-0000-00000000c{suffix:03}")
    });
    row.revocation_source = None;
    row.inheritance_path = json!([]);
    row.transfer_behavior = Value::Null;
}

fn permission_row_by_registration(rows: &[Value], resource_id: Uuid) -> &Value {
    let registration_id = resource_id.to_string();
    rows.iter()
        .find(|row| row["registration_id"] == json!(registration_id))
        .expect("permission row must exist")
}

fn permission_row_by_scope_kind<'a>(rows: &'a [Value], kind: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["grant_scope"]["kind"] == json!(kind))
        .unwrap_or_else(|| panic!("permission row with scope kind {kind} must exist"))
}

fn v2_permissions_current_resource_id() -> Uuid {
    Uuid::from_u128(0xe100)
}

fn v2_permissions_stale_resource_id() -> Uuid {
    Uuid::from_u128(0xe200)
}

fn v2_permissions_predecessor_resource_id() -> Uuid {
    Uuid::from_u128(0xe300)
}
