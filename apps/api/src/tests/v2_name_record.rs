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

#[tokio::test]
async fn v2_get_name_uses_sepolia_positioned_at_token_on_mixed_checkpoints() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_checkpoint_names(&database).await?;

    let at = v2_sepolia_snapshot_token();
    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={at}"),
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
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));
    assert_eq!(payload["data"]["chain_id"], json!(11155111));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_at_tokens_round_trip_mainnet_and_sepolia_profiles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_checkpoint_names(&database).await?;

    let mainnet = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;
    let mainnet_at =
        v2_at_token_from_meta_as_of(&mainnet, "1", "ethereum", "ethereum-mainnet")?;
    let mainnet_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}?at={mainnet_at}"),
    )
    .await?;
    assert_eq!(mainnet_replay["meta"]["as_of"], mainnet["meta"]["as_of"]);
    assert_eq!(mainnet_replay["data"], mainnet["data"]);

    let sepolia_at = v2_sepolia_snapshot_token();
    let sepolia = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_at}"),
    )
    .await?;
    let sepolia_replay_at = v2_at_token_from_meta_as_of(
        &sepolia,
        "11155111",
        "ethereum-sepolia",
        "ethereum-sepolia",
    )?;
    let sepolia_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_replay_at}"),
    )
    .await?;
    assert_eq!(sepolia_replay["meta"]["as_of"], sepolia["meta"]["as_of"]);
    assert_eq!(sepolia_replay["data"], sepolia["data"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_without_at_keeps_mainnet_preference_on_mixed_checkpoints() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_checkpoint_names(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": V2_MAINNET_SNAPSHOT_BLOCK,
            "block_hash": V2_MAINNET_SNAPSHOT_HASH,
            "timestamp": V2_MAINNET_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("11155111").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum"));
    assert_eq!(payload["data"]["chain_id"], json!(1));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_timestamp_at_uses_sepolia_when_only_sepolia_checkpoint_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_only_checkpoint_name(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?at=2026-04-17T00:10:30Z"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_returns_indexed_values() -> Result<()> {
    let payload = v2_name_records_payload("/v2/names/Alice.eth/records").await?;

    assert!(payload["data"].get("records").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["data"]["resolver"],
        json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })
    );
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["text_records"],
        json!({
            "avatar": "https://example.test/avatar.png",
            "description": "Alice profile"
        })
    );
    assert_eq!(payload["data"]["content_hash"], json!("ipfs://alice"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_keys_filter_values_and_per_key_answers() -> Result<()> {
    let payload =
        v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60,text:description")
            .await?;

    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["text_records"],
        json!({
            "description": "Alice profile"
        })
    );
    assert_eq!(payload["data"]["content_hash"], Value::Null);
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:description": {
                "status": "ok",
                "value": "Alice profile"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_rejects_too_many_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let keys = (0..=200)
        .map(|index| format!("text:key{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let uri = format!("/v2/names/alice.eth/records?keys={keys}");

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 oversized records keys request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("keys must contain at most 200 record keys")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_reports_unset_and_unsupported_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=contenthash,text:email",
        |_, _, inventory| {
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
                }
            ]);
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
                }
            ]);
            inventory.explicit_gaps = json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ]);
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(payload["data"]["content_hash"], Value::Null);
    assert_eq!(
        payload["data"]["records"],
        json!({
            "contenthash": {
                "status": "not_found",
                "failure_reason": "not_observed_on_current_resolver"
            },
            "text:email": {
                "status": "unsupported",
                "unsupported_reason": "resolver_family_pending"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_include_inventory_uses_product_key_lists() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=contenthash,text:email&include=inventory",
        |_, _, inventory| {
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
                }
            ]);
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
                }
            ]);
            inventory.explicit_gaps = json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ]);
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60", "avatar"],
            "unset_keys": ["contenthash"],
            "unsupported_keys": ["text:email"]
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_partitions_unsupported_entries() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=addr:60,avatar&include=inventory",
        |_, _, inventory| {
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
                }
            ]);
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
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        None,
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60"],
            "unset_keys": [],
            "unsupported_keys": ["avatar"]
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_absence_is_unknown_not_unsupported() -> Result<()> {
    let payload =
        v2_name_records_payload_without_inventory("/v2/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": [],
            "unset_keys": [],
            "unsupported_keys": []
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reads_persisted_verified_values() -> Result<()> {
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000fed"
            },
            "provenance": {
                "execution_trace_id": Uuid::from_u128(0x0e7ec7ace00000000000000000000072).to_string()
            }
        }
    ]);
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
        Some((&["addr:60"], verified_queries)),
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000fed"
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000fed"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_executes_on_demand_for_cache_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row
        })
        .await?;
    database
        .insert_record_inventory_current_row({
            let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
            inventory.record_version_boundary["chain_position"]["block_hash"] =
                json!(execution_block_hash);
            inventory.chain_positions = json!({
                "ethereum-mainnet": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            inventory
        })
        .await?;
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": executed_address
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(rpc_requests[0]["method"], json!("eth_call"));
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        json!(bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS)
    );
    assert_eq!(
        rpc_requests[0]["params"][1],
        json!({
            "blockHash": execution_block_hash,
            "requireCanonical": true
        })
    );

    let cached_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 cached verified name records request failed")?;
    assert_eq!(cached_response.status(), StatusCode::OK);
    let cached_payload: Value = read_json(cached_response).await?;
    assert_eq!(
        cached_payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_cache_miss_reports_stale_when_execution_cannot_run(
) -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "verified_answer_stale_for_snapshot"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_maps_stale_lookup_storage_reason_to_product_reason(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;
    let (rpc_url, rpc_handle) = spawn_v2_name_records_error_mock_rpc(vec![(
        -32000,
        "state not available: record_inventory_current projection does not match the selected snapshot",
    )])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 stale verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "verified_answer_stale_for_snapshot"
        })
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_stale_for_non_ens_on_demand_miss(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9220);
    let token_lineage_id = Uuid::from_u128(0x9221);
    let surface_binding_id = Uuid::from_u128(0x9222);

    database
        .seed_name_current_binding(
            logical_name_id,
            "basenames",
            "alice.base.eth",
            "Alice.base.eth",
            "namehash:alice.base.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            row.namespace = "basenames".to_owned();
            row.canonical_display_name = "Alice.base.eth".to_owned();
            row.normalized_name = "alice.base.eth".to_owned();
            row.namehash = "namehash:alice.base.eth".to_owned();
            row.declared_summary = json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "resolver": {
                    "chain_id": "base-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged"
                }
            });
            row.provenance = json!({
                "manifest_versions": [
                    {
                        "manifest_version": 2,
                        "source_family": "basenames_execution",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "basenames_v1"
                    }
                ]
            });
            row.chain_positions = json!({
                "base": {
                    "chain_id": "base-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbase-binding",
                    "timestamp": "2026-04-17T00:00:03Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbinding",
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row.canonicality_summary = json!({
                "status": "finalized",
                "chains": {
                    "base-mainnet": "finalized",
                    "ethereum-mainnet": "finalized"
                }
            });
            row
        })
        .await?;
    database
        .insert_record_inventory_current_row({
            let mut inventory =
                basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
            inventory.chain_positions = json!({
                "base": {
                    "chain_id": "base-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbase-binding",
                    "timestamp": "2026-04-17T00:00:03Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbinding",
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            inventory.canonicality_summary = json!({
                "status": "finalized",
                "chains": {
                    "base-mainnet": "finalized",
                    "ethereum-mainnet": "finalized"
                }
            });
            inventory
        })
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.base.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 non-ENS on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "verified_answer_stale_for_snapshot"
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_unsupported_without_verified_boundary(
) -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=avatar",
        |row| {
            row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ObservedOnly);
        },
        |_, _, _| {},
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["avatar"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "verified_records_not_supported"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_falls_back_to_verified_when_indexed_does_not_satisfy(
) -> Result<()> {
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000ee"
            },
            "provenance": {
                "execution_trace_id": Uuid::from_u128(0x0e7ec7ace00000000000000000000073).to_string()
            }
        }
    ]);
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=auto&keys=addr:60",
        |logical_name_id, resource_id, inventory| {
            *inventory = dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            );
        },
        Some((&["addr:60"], verified_queries)),
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x00000000000000000000000000000000000000ee"
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": "0x00000000000000000000000000000000000000ee"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_blends_indexed_and_verified_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=auto&keys=addr:60,text:email",
        |_, _, inventory| {
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:email": {
                "status": "stale",
                "failure_reason": "verified_answer_stale_for_snapshot"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_missing_name_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/missing.eth/records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing name records request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_response_omits_banned_v1_spellings() -> Result<()> {
    let payload =
        v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_uses_envelope_shape() -> Result<()> {
    let payload = v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60").await?;

    assert!(payload.get("page").is_none());
    assert!(payload["data"].is_object());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_returns_record_shaped_rows_in_display_name_order() -> Result<()> {
    let (database, payload) =
        v2_subnames_payload("/v2/names/Parent.eth/subnames?page_size=3").await?;

    assert_eq!(payload["page"]["page_size"], json!(3));
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
        .expect("subnames data must be an array");
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["name"], json!("alpha.parent.eth"));
    assert_eq!(data[1]["name"], json!("beta.parent.eth"));
    assert_eq!(data[2]["name"], json!("gamma.parent.eth"));
    assert_eq!(data[0]["display_name"], json!("Alpha.Parent.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(data[0]["namehash"], json!("node:alpha.parent.eth"));
    assert_eq!(
        data[0]["labelhash"],
        json!(labelhash_for_display_name("alpha.parent.eth"))
    );
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000aa")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000ab")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T03:04:05Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T03:04:05Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T03:04:05Z"));
    assert_eq!(data[1]["registration_status"], json!("released"));
    assert_eq!(data[2]["registration_status"], json!("unregistered"));
    assert!(data[0].get("subname_count").is_none());
    assert!(data[0].get("resolver").is_none());
    assert!(data[0].get("addresses").is_none());
    assert!(data[0].get("text_records").is_none());
    assert!(data[0].get("content_hash").is_none());
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_paginates_with_opaque_cursor_without_overlap() -> Result<()> {
    let (database, first_page) =
        v2_subnames_payload("/v2/names/parent.eth/subnames?page_size=2").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_subnames_payload_for_database(
        &database,
        &format!("/v2/names/parent.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["next_cursor"], Value::Null);
    assert_eq!(second_page["page"]["has_more"], json!(false));
    assert_eq!(
        first_page["data"]
            .as_array()
            .expect("first page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["alpha.parent.eth", "beta.parent.eth"]
    );
    assert_eq!(
        second_page["data"]
            .as_array()
            .expect("second page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["gamma.parent.eth"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_cursor_reused_for_different_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_v2_subnames_bound_child(
        &database,
        "ens:other.eth",
        "other.eth",
        "node:other.eth",
        79,
        Uuid::from_u128(0x4030),
        Uuid::from_u128(0x5030),
        Uuid::from_u128(0x6030),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000002"
            }
        }),
    )
    .await?;
    seed_v2_subnames_bound_child(
        &database,
        "ens:one.other.eth",
        "one.other.eth",
        "node:one.other.eth",
        80,
        Uuid::from_u128(0x4040),
        Uuid::from_u128(0x5040),
        Uuid::from_u128(0x6040),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000003"
            }
        }),
    )
    .await?;
    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[v2_subnames_declared_child_row(
            "ens:other.eth",
            "ens:one.other.eth",
            "one.other.eth",
            "node:one.other.eth",
            905,
            80,
        )],
    )
    .await?;

    let first_page =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?page_size=2")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let response = v2_subnames_response_for_database(
        &database,
        &format!("/v2/names/other.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_include_counts_adds_child_subname_count_only_when_requested()
-> Result<()> {
    let (database, without_counts) =
        v2_subnames_payload("/v2/names/parent.eth/subnames?page_size=3").await?;
    assert!(
        without_counts["data"][0].get("subname_count").is_none(),
        "subname_count must be omitted by default"
    );

    let with_counts =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?include=counts")
            .await?;
    assert_eq!(with_counts["data"][0]["subname_count"], json!(1));
    assert_eq!(with_counts["data"][1]["subname_count"], json!(0));
    assert_eq!(with_counts["data"][2]["subname_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_parent_with_zero_children_returns_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_parent(&database, "ens:empty.eth", "empty.eth", "node:empty.eth", 80).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v2/names/empty.eth/subnames").await?;

    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_missing_parent_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/missing.eth/subnames")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing parent subnames request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_malformed_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/parent.eth/subnames?cursor=not-a-cursor")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 malformed subnames cursor request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_wrong_sort_or_snapshot_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let wrong_sort = crate::v2::encode(&crate::v2::CursorPayload::new(
        "wrong",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("parent".to_owned(), "ens:parent.eth".to_owned()),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                "ens:alpha.parent.eth".to_owned(),
            ),
        ]),
        Some("wrong-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/names/parent.eth/subnames?cursor={wrong_sort}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 wrong-sort subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let wrong_snapshot = crate::v2::encode(&crate::v2::CursorPayload::new(
        "display_name_asc",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("parent".to_owned(), "ens:parent.eth".to_owned()),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                "ens:alpha.parent.eth".to_owned(),
            ),
        ]),
        Some("wrong-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/names/parent.eth/subnames?cursor={wrong_snapshot}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 wrong-snapshot subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}

const V2_MAINNET_SNAPSHOT_NAME: &str = "mainnet-pin.eth";
const V2_MAINNET_SNAPSHOT_HASH: &str = "0xv2-mainnet-pin";
const V2_MAINNET_SNAPSHOT_BLOCK: i64 = 21_000_011;
const V2_MAINNET_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:00:11Z";
const V2_SEPOLIA_SNAPSHOT_NAME: &str = "sepolia-pin.eth";
const V2_SEPOLIA_SNAPSHOT_HASH: &str = "0xv2-sepolia-pin";
const V2_SEPOLIA_SNAPSHOT_BLOCK: i64 = 111_551_110;
const V2_SEPOLIA_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:10Z";
const V2_SEPOLIA_ONLY_SNAPSHOT_NAME: &str = "sepolia-only.eth";
const V2_SEPOLIA_ONLY_SNAPSHOT_HASH: &str = "0xv2-sepolia-only";
const V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK: i64 = 111_551_120;
const V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:20Z";

async fn seed_v2_mixed_checkpoint_names(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_SNAPSHOT_NAME,
        "SepoliaPin.eth",
        "namehash:sepolia-pin.eth",
        Uuid::from_u128(0x7e20),
        Uuid::from_u128(0x7e21),
        Uuid::from_u128(0x7e22),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    seed_v2_snapshot_profile_name(
        database,
        V2_MAINNET_SNAPSHOT_NAME,
        "MainnetPin.eth",
        "namehash:mainnet-pin.eth",
        Uuid::from_u128(0x7e10),
        Uuid::from_u128(0x7e11),
        Uuid::from_u128(0x7e12),
        "ethereum",
        "ethereum-mainnet",
        V2_MAINNET_SNAPSHOT_BLOCK,
        V2_MAINNET_SNAPSHOT_HASH,
        V2_MAINNET_SNAPSHOT_TIMESTAMP,
    )
    .await
}

async fn seed_v2_sepolia_only_checkpoint_name(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_ONLY_SNAPSHOT_NAME,
        "SepoliaOnly.eth",
        "namehash:sepolia-only.eth",
        Uuid::from_u128(0x7e30),
        Uuid::from_u128(0x7e31),
        Uuid::from_u128(0x7e32),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
        V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
        V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    sqlx::query("DELETE FROM chain_checkpoints WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.pool)
        .await
        .context("failed to remove mainnet checkpoint for sepolia-only v2 snapshot test")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_v2_snapshot_profile_name(
    database: &TestDatabase,
    normalized_name: &str,
    display_name: &str,
    namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    let logical_name_id = format!("ens:{normalized_name}");
    seed_v2_subnames_bound_child(
        database,
        &logical_name_id,
        display_name,
        namehash,
        block_number.rem_euclid(100),
        resource_id,
        token_lineage_id,
        surface_binding_id,
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "ens_v2_registry"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000001"
            },
            "resolver": {
                "chain_id": chain_id,
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        }),
    )
    .await?;

    let mut row = bigname_storage::load_name_current(&database.pool, &logical_name_id)
        .await
        .with_context(|| format!("failed to load v2 snapshot fixture row {logical_name_id}"))?
        .with_context(|| format!("v2 snapshot fixture row {logical_name_id} was not inserted"))?;
    row.chain_positions = v2_snapshot_chain_positions(slot, chain_id, block_number, block_hash, timestamp);
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            chain_id: "finalized"
        }
    });
    row.declared_summary["resolver"]["chain_id"] = json!(chain_id);
    row.provenance["manifest_versions"] = json!([
        {
            "manifest_version": 3,
            "source_family": "ens_v2_registry_l1",
            "chain": chain_id,
            "deployment_epoch": "ens_v2"
        }
    ]);
    database.insert_name_current_row(row).await
}

fn v2_snapshot_chain_positions(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Value {
    json!({
        slot: {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp
        }
    })
}

fn v2_sepolia_snapshot_token() -> String {
    v2_at_token(
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .expect("sepolia snapshot token fixture must encode")
}

fn v2_at_token_from_meta_as_of(
    payload: &Value,
    numeric_chain_id: &str,
    slot: &str,
    chain_id: &str,
) -> Result<String> {
    let as_of = payload
        .pointer(&format!("/meta/as_of/{numeric_chain_id}"))
        .with_context(|| format!("response must include meta.as_of[{numeric_chain_id}]"))?;
    let block_number = as_of
        .get("block_number")
        .and_then(Value::as_i64)
        .context("meta.as_of block_number must be an i64")?;
    let block_hash = as_of
        .get("block_hash")
        .and_then(Value::as_str)
        .context("meta.as_of block_hash must be a string")?;
    let timestamp = as_of
        .get("timestamp")
        .and_then(Value::as_str)
        .context("meta.as_of timestamp must be a string")?;

    v2_at_token(slot, chain_id, block_number, block_hash, timestamp)
}

fn v2_at_token(
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

async fn v2_name_record_payload(uri: &str) -> Result<Value> {
    v2_name_record_payload_with_row(uri, |_| {}).await
}

async fn v2_name_record_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
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
    read_json(response).await
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

async fn v2_name_records_payload(uri: &str) -> Result<Value> {
    v2_name_records_payload_with_setup(uri, |_, _, _| {}, None).await
}

async fn v2_name_records_payload_with_setup(
    uri: &str,
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<Value> {
    v2_name_records_payload_with_row_and_setup(uri, |_| {}, configure_inventory, verified).await
}

async fn v2_name_records_payload_with_row_and_setup(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<Value> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture_with_row(
        &database,
        configure_row,
        configure_inventory,
        verified,
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn v2_name_records_payload_without_inventory(uri: &str) -> Result<Value> {
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
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name records request without inventory failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn spawn_v2_name_records_error_mock_rpc(
    responses: Vec<(i64, &'static str)>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock v2 name-records RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (code, message) in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock v2 name-records RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_v2_name_records_mock_rpc_error(&mut socket, code, message).await?;
        }
        Ok(requests)
    });

    Ok((url, handle))
}

async fn write_v2_name_records_mock_rpc_error(
    socket: &mut tokio::net::TcpStream,
    code: i64,
    message: &str,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": code,
            "message": message,
        },
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock v2 name-records RPC response")
}

async fn seed_v2_alice_name_records_fixture(
    database: &TestDatabase,
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<()> {
    seed_v2_alice_name_records_fixture_with_row(database, |_| {}, configure_inventory, verified)
        .await
}

async fn seed_v2_alice_name_records_fixture_with_row(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<()> {
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
    database
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            configure_row(&mut row);
            row
        })
        .await?;

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
    configure_inventory(logical_name_id, resource_id, &mut inventory);
    database.insert_record_inventory_current_row(inventory).await?;

    if let Some((record_keys, verified_queries)) = verified {
        let request_key = resolution_execution_request_key(record_keys);
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000070);
        let trace = resolution_execution_trace(
            execution_trace_id,
            &request_key,
            record_keys,
            verified_queries.clone(),
        );
        let outcome = resolution_execution_outcome(
            execution_trace_id,
            &request_key,
            verified_queries,
            logical_name_id,
            resource_id,
        );
        upsert_execution_trace(&database.pool, &trace).await?;
        upsert_execution_outcome(&database.pool, &outcome).await?;
    }

    Ok(())
}

async fn v2_subnames_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let payload = v2_subnames_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_subnames_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_subnames_response_for_database(database, uri).await?;

    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_subnames_response_for_database(
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
        .context("v2 subnames request failed")
}

async fn seed_v2_subnames_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_subnames_parent(database, "ens:parent.eth", "parent.eth", "node:parent.eth", 80)
        .await?;
    seed_v2_subnames_bound_child(
        database,
        "ens:alpha.parent.eth",
        "Alpha.Parent.eth",
        "node:alpha.parent.eth",
        81,
        Uuid::from_u128(0x4010),
        Uuid::from_u128(0x5010),
        Uuid::from_u128(0x6010),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": "0x00000000000000000000000000000000000000aB",
                "registered_at": "2024-01-02T03:04:05Z",
                "created_at": "2023-01-02T03:04:05Z",
                "expiry": "2027-01-02T03:04:05Z"
            },
            "control": {
                "registry_owner": "0x00000000000000000000000000000000000000aA"
            }
        }),
    )
    .await?;
    seed_v2_subnames_bound_child(
        database,
        "ens:beta.parent.eth",
        "beta.parent.eth",
        "node:beta.parent.eth",
        82,
        Uuid::from_u128(0x4020),
        Uuid::from_u128(0x5020),
        Uuid::from_u128(0x6020),
        json!({
            "registration": {
                "status": "released",
                "authority_kind": "registrar",
                "released_at": "2026-02-03T04:05:06Z",
                "registrant": "0x00000000000000000000000000000000000000bB"
            },
            "control": {
                "registry_owner": "0x00000000000000000000000000000000000000bA"
            }
        }),
    )
    .await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:gamma.parent.eth",
            "gamma.parent.eth",
            "node:gamma.parent.eth",
            83,
        )],
    )
    .await?;
    database
        .insert_name_current_row(v2_subnames_name_current_row(
            "ens:gamma.parent.eth",
            "gamma.parent.eth",
            "node:gamma.parent.eth",
            83,
            None,
            None,
            None,
            json!({}),
        ))
        .await?;

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:delta.alpha.parent.eth",
            "delta.alpha.parent.eth",
            "node:delta.alpha.parent.eth",
            84,
        )],
    )
    .await?;

    bigname_storage::upsert_children_current_rows(
        &database.pool,
        &[
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:gamma.parent.eth",
                "gamma.parent.eth",
                "node:gamma.parent.eth",
                903,
                83,
            ),
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:beta.parent.eth",
                "beta.parent.eth",
                "node:beta.parent.eth",
                902,
                82,
            ),
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:alpha.parent.eth",
                "Alpha.Parent.eth",
                "node:alpha.parent.eth",
                901,
                81,
            ),
            v2_subnames_declared_child_row(
                "ens:alpha.parent.eth",
                "ens:delta.alpha.parent.eth",
                "delta.alpha.parent.eth",
                "node:delta.alpha.parent.eth",
                904,
                84,
            ),
        ],
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 80,
                "block_hash": "0xname50",
                "timestamp": "2026-04-17T00:00:20Z"
            }
        }))
        .await?;

    Ok(())
}

async fn seed_v2_subnames_parent(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> Result<()> {
    seed_v2_subnames_bound_child(
        database,
        logical_name_id,
        display_name,
        namehash,
        block_number,
        Uuid::from_u128(0x4000),
        Uuid::from_u128(0x5000),
        Uuid::from_u128(0x6000),
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

#[allow(clippy::too_many_arguments)]
async fn seed_v2_subnames_bound_child(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    declared_summary: Value,
) -> Result<()> {
    let normalized_name = normalized_name_from_logical_name_id(logical_name_id);
    let mut surface = collection_name_surface(
        logical_name_id,
        display_name,
        namehash,
        block_number,
    );
    surface.normalized_name = normalized_name.to_owned();
    surface.dns_encoded_name = normalized_name.as_bytes().to_vec();
    surface.labelhashes = labelhash_for_display_name(normalized_name)
        .into_iter()
        .collect();

    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[surface],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            token_lineage_id,
            &format!("0xtoken{block_number:02x}"),
            block_number,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            &format!("0xresource{block_number:02x}"),
            block_number,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            &format!("0xbinding{block_number:02x}"),
            block_number,
            1_717_176_000 + block_number,
        )],
    )
    .await?;
    database
        .insert_name_current_row(v2_subnames_name_current_row(
            logical_name_id,
            display_name,
            namehash,
            block_number,
            Some(surface_binding_id),
            Some(resource_id),
            Some(token_lineage_id),
            declared_summary,
        ))
        .await
}

fn v2_subnames_declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let mut row = declared_child_row(
        parent_logical_name_id,
        child_logical_name_id,
        display_name,
        namehash,
        normalized_event_id,
        block_number,
    );
    row.normalized_name = normalized_name_from_logical_name_id(child_logical_name_id).to_owned();
    row.labelhash = labelhash_for_display_name(&row.normalized_name);
    row
}

#[allow(clippy::too_many_arguments)]
fn v2_subnames_name_current_row(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    surface_binding_id: Option<Uuid>,
    resource_id: Option<Uuid>,
    token_lineage_id: Option<Uuid>,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    let (namespace, normalized_name) = split_logical_name_id(logical_name_id);
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: surface_binding_id
            .is_some()
            .then_some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "name_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [source_family_for_namespace(namespace)],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_176_000 + block_number),
    }
}

fn split_logical_name_id(logical_name_id: &str) -> (&str, &str) {
    logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace")
}

fn normalized_name_from_logical_name_id(logical_name_id: &str) -> &str {
    split_logical_name_id(logical_name_id).1
}

fn resolution_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex(bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS),
        resolution_left_pad_hex("20", 64),
        resolution_padded_address_hex(address),
    ))
}

fn resolution_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    resolution_left_pad_hex(stripped, 64)
}

fn resolution_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
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
