#[tokio::test]
async fn v2_get_name_returns_flat_name_record_envelope() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth").await?;

    assert!(payload.get("page").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("name"), Some(&json!("alice.eth")));
    assert_eq!(data.get("display_name"), Some(&json!("Alice.eth")));
    assert_eq!(data.get("namespace"), Some(&json!("ens")));
    assert_eq!(data.get("namehash"), Some(&json!("namehash:alice.eth")));
    assert_eq!(data.get("registration_status"), Some(&json!("active")));
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("chain_id"), Some(&json!(1)));
    assert_eq!(data.get("network"), Some(&json!("ethereum")));
    assert_eq!(
        data.get("resolver"),
        Some(&json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        }))
    );
    assert_eq!(
        data.get("registration_id"),
        Some(&json!(Uuid::from_u128(0x2200).to_string()))
    );
    assert_eq!(data.get("token_id"), Some(&Value::Null));
    assert_eq!(
        data.get("owner"),
        Some(&json!("0x00000000000000000000000000000000000000bb"))
    );
    assert_eq!(data.get("manager"), Some(&Value::Null));
    assert_eq!(
        data.get("registrant"),
        Some(&json!("0x00000000000000000000000000000000000000aa"))
    );
    assert_eq!(data.get("registered_at"), Some(&json!("2024-01-02T03:04:05Z")));
    assert_eq!(data.get("created_at"), Some(&json!("2023-01-02T03:04:05Z")));
    assert_eq!(data.get("expires_at"), Some(&json!("2027-01-02T03:04:05Z")));
    assert_eq!(
        data.get("addresses"),
        Some(&json!({
            "60": "0x0000000000000000000000000000000000000def"
        }))
    );
    assert_eq!(
        data.get("text_records"),
        Some(&json!({
            "avatar": "https://example.test/avatar.png",
            "description": "Alice profile"
        }))
    );
    assert_eq!(data.get("content_hash"), Some(&json!("ipfs://alice")));
    assert_eq!(data.get("primary_name"), Some(&json!("alice.eth")));
    assert_eq!(
        data.get("primary_address"),
        Some(&json!("0x0000000000000000000000000000000000000def"))
    );
    assert!(data.get("unsupported_fields").is_none());

    Ok(())
}

#[tokio::test]
async fn v2_get_name_response_omits_banned_v1_spellings() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth").await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_uses_in_record_failed_status() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth?source=verified").await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("failed"));
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_ens_v2_registry_as_registered() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"] = json!({
            "status": "active",
            "authority_kind": "ens_v2_registry",
            "authority_key": "registry:ens-v2:alice",
            "released_at": null,
            "registrant": null,
            "latest_event_kind": "NameTransferred"
        });
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("registered"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_released_as_released() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"] = json!({
            "status": "released",
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:alice",
            "released_at": "2026-06-14T00:00:00Z",
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expiry": "2026-03-01T00:00:00Z",
            "latest_event_kind": "RegistrationReleased"
        });
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("released"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_no_binding_as_unregistered() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.surface_binding_id = None;
        row.resource_id = None;
        row.token_lineage_id = None;
        row.binding_kind = None;
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("unregistered"));
    assert_eq!(payload["data"]["registration_id"], Value::Null);

    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_source_auto() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.eth?source=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 source=auto request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("source must be one of: indexed, verified")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_reads_basenames_record_with_base_network() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9200);
    let token_lineage_id = Uuid::from_u128(0x9201);
    let surface_binding_id = Uuid::from_u128(0x9202);

    database
        .seed_basenames_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 basenames name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["namespace"], json!("basenames"));
    assert_eq!(payload["data"]["network"], json!("base"));
    assert_eq!(payload["data"]["chain_id"], json!(8453));
    assert_eq!(payload["data"]["registration_status"], json!("active"));
    assert_eq!(payload["data"]["resolver"]["chain_id"], json!(8453));

    database.cleanup().await?;
    Ok(())
}

async fn v2_name_record_payload(uri: &str) -> Result<Value> {
    v2_name_record_payload_with_row(uri, |_| {}).await
}

async fn v2_name_record_payload_with_row(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
) -> Result<Value> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary["registration"] = json!({
        "status": "active",
        "authority_kind": "registrar",
        "authority_key": "registrar:ethereum-mainnet:alice",
        "released_at": null,
        "registrant": "0x00000000000000000000000000000000000000aa",
        "expiry": "2027-01-02T03:04:05Z",
        "registered_at": "2024-01-02T03:04:05Z",
        "created_at": "2023-01-02T03:04:05Z",
        "latest_event_kind": "NameRegistered"
    });
    row.declared_summary["control"] = json!({
        "status": "active",
        "expiry": "2027-01-02T03:04:05Z",
        "registry_owner": "0x00000000000000000000000000000000000000bb",
        "registrant": "0x00000000000000000000000000000000000000aa",
        "latest_event_kind": "NameTransferred"
    });
    row.declared_summary["primary_name"] = json!("alice.eth");
    configure_row(&mut row);
    database.insert_name_current_row(row).await?;

    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "cacheable": true
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "cacheable": true
        }
    ]);
    inventory.explicit_gaps = json!([]);
    inventory.unsupported_families = json!([]);
    inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000def"
            }
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "https://example.test/avatar.png"
            }
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://alice"
            }
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "status": "success",
            "value": {
                "key": "description",
                "value": "Alice profile"
            }
        }
    ]);
    database.insert_record_inventory_current_row(inventory).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

fn assert_no_banned_v1_spellings(value: &Value) {
    const BANNED: &[&str] = &[
        "as_of_timestamp",
        "canonical_display_name",
        "chain_positions",
        "coin_addresses",
        "coin_type_addresses",
        "consistency",
        "coverage",
        "declared_state",
        "expiration",
        "expiry",
        "expiry_date",
        "last_updated",
        "logical_name_id",
        "manager_address",
        "normalized_name",
        "owner_address",
        "provenance",
        "resolver_address",
        "resource_id",
        "surface_binding_id",
        "token_lineage_id",
        "verified_state",
    ];

    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !BANNED.contains(&key.as_str()),
                    "v2 response leaked banned v1 field {key}"
                );
                assert_no_banned_v1_spellings(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_banned_v1_spellings(value);
            }
        }
        _ => {}
    }
}
