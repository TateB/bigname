struct DiagnosticRouteCase {
    suffix: &'static str,
    expected_data: Value,
}

fn diagnostic_route_cases() -> Vec<DiagnosticRouteCase> {
    vec![
        DiagnosticRouteCase {
            suffix: "coverage",
            expected_data: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ens_v1_registry_l1"],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            }),
        },
        DiagnosticRouteCase {
            suffix: "binding",
            expected_data: json!({
                "anchors": {
                    "logical_name_id": "ens:alice.eth",
                    "namehash": "namehash:alice.eth",
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100"
                },
                "surface_binding": {
                    "surface_binding_id": "00000000-0000-0000-0000-000000003300",
                    "binding_kind": "declared_registry_path"
                },
                "history": {
                    "latest_event_kind": "NameTransferred"
                }
            }),
        },
        DiagnosticRouteCase {
            suffix: "authority",
            expected_data: json!({
                "authority": {
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100",
                    "binding_kind": "declared_registry_path"
                },
                "control": {
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "registry_owner": "0x00000000000000000000000000000000000000bb",
                    "latest_event_kind": "NameTransferred"
                },
                "permission_lineage": {
                    "status": "unsupported",
                    "unsupported_reason": "permission_lineage_not_projected_on_name_current"
                }
            }),
        },
    ]
}

#[tokio::test]
async fn v2_diagnostics_name_routes_return_declared_state_slices() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    for case in diagnostic_route_cases() {
        let uri = format!("/v2/diagnostics/names/Alice.eth/{}", case.suffix);
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert!(payload.get("page").is_none(), "{uri}");
        assert_eq!(payload["data"], case.expected_data, "{uri}");
        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xdiag1406f43",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_coverage_synthesizes_missing_unsupported_reason() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x4400);
    let token_lineage_id = Uuid::from_u128(0x5500);
    let surface_binding_id = Uuid::from_u128(0x6600);
    let logical_name_id = "ens:unsupported.eth";
    let normalized_name = "unsupported.eth";

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            normalized_name,
            normalized_name,
            &format!("namehash:{normalized_name}"),
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let mut row = diagnostic_name_current_row(
        logical_name_id,
        21_000_004,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    );
    row.coverage = json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "exact_name",
        "unsupported_reason": null
    });
    database.insert_name_current_row(row).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/unsupported.eth/coverage",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(payload["data"]["status"], json!("unsupported"));
    assert_eq!(
        payload["data"]["unsupported_reason"],
        json!("name_coverage_unsupported_reason_missing")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_returns_sections_comparison_and_value_sources() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let verified_value = "0x0000000000000000000000000000000000000fed";
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
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
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        Some((
            &["addr:60"],
            json!([
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": verified_value
                    },
                    "provenance": {
                        "execution_trace_id": Uuid::from_u128(0x0e7ec7ace00000000000000000000080).to_string()
                    }
                }
            ]),
        )),
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/Alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert!(payload.get("page").is_none());
    assert_eq!(
        payload["data"]["record_inventory"]["selectors"],
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }
        ])
    );
    assert_eq!(
        payload["data"]["record_cache"]["entries"],
        json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000def"
                }
            }
        ])
    );
    assert_eq!(
        payload["data"]["comparison"]["addr:60"],
        json!({
            "indexed": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "verified": {
                "status": "ok",
                "value": verified_value
            }
        })
    );
    assert_eq!(
        payload["data"]["value_sources"]["addr:60"],
        json!([
            {
                "source": "indexed",
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            {
                "source": "verified",
                "status": "ok",
                "value": verified_value
            }
        ])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_keys_scope_comparison_exactly() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000081);
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, _| {},
        Some((
            &["contenthash", "text:description"],
            json!([
                {
                    "record_key": "contenthash",
                    "status": "success",
                    "value": {
                        "value": "ipfs://verified-alice"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:description",
                    "status": "success",
                    "value": {
                        "key": "description",
                        "value": "Verified Alice profile"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]),
        )),
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/Alice.eth/records?keys=contenthash,text:description",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["contenthash".to_owned(), "text:description".to_owned()]
    );
    assert_eq!(
        payload["data"]["comparison"]["contenthash"]["verified"],
        json!({
            "status": "ok",
            "value": "ipfs://verified-alice"
        })
    );
    assert_eq!(
        payload["data"]["comparison"]["text:description"]["verified"],
        json!({
            "status": "ok",
            "value": "Verified Alice profile"
        })
    );
    assert!(
        payload["data"].get("comparison_explicit_gaps").is_none(),
        "keys-scoped comparisons must not report default-cap truncation"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_caps_default_comparison_and_lists_explicit_gaps() -> Result<()>
{
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000082);
    let verified_record_keys = diagnostic_text_record_keys(16);
    let verified_record_key_refs = verified_record_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = Value::Array(diagnostic_text_record_selectors(18));
            inventory.entries = Value::Array(diagnostic_text_record_entries(18));
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        Some((
            verified_record_key_refs.as_slice(),
            diagnostic_text_verified_queries(16, execution_trace_id),
        )),
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/Alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    let comparison = payload["data"]["comparison"]
        .as_object()
        .expect("comparison must be an object");
    assert_eq!(comparison.len(), 16);
    assert!(comparison.contains_key("text:key00"));
    assert!(comparison.contains_key("text:key15"));
    assert!(!comparison.contains_key("text:key16"));
    assert!(!comparison.contains_key("text:key17"));
    assert_eq!(
        comparison["text:key00"]["verified"],
        json!({
            "status": "ok",
            "value": "verified-00"
        })
    );
    assert_eq!(
        comparison["text:key15"]["verified"],
        json!({
            "status": "ok",
            "value": "verified-15"
        })
    );
    assert_eq!(
        payload["data"]["comparison_explicit_gaps"],
        json!([
            {
                "record_key": "text:key16",
                "record_family": "text",
                "selector_key": "key16",
                "gap_reason": "diagnostics_comparison_default_limit_exceeded"
            },
            {
                "record_key": "text:key17",
                "record_family": "text",
                "selector_key": "key17",
                "gap_reason": "diagnostics_comparison_default_limit_exceeded"
            }
        ])
    );
    assert_eq!(
        payload["data"]["record_inventory"]["selectors"]
            .as_array()
            .expect("selectors must be an array")
            .len(),
        18
    );
    assert_eq!(
        payload["data"]["record_cache"]["entries"]
            .as_array()
            .expect("record cache entries must be an array")
            .len(),
        18
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_at_or_below_cap_has_no_truncation_note() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = Value::Array(diagnostic_text_record_selectors(16));
            inventory.entries = Value::Array(diagnostic_text_record_entries(16));
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        None,
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/Alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .len(),
        16
    );
    assert!(payload["data"].get("comparison_explicit_gaps").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_reuses_supported_inventory_boundary_fallback() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );
    let expected_boundary = json!({
        "namespace": "ens",
        "name": "alice.eth",
        "registration_id": resource_id.to_string(),
        "normalized_event_id": 1201,
        "event_kind": "RecordVersionChanged",
        "chain_position": worker_boundary["chain_position"].clone(),
    });

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
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["record_inventory"]["record_version_boundary"],
        expected_boundary
    );
    assert_eq!(
        payload["data"]["record_cache"]["record_version_boundary"],
        expected_boundary
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_cache_keeps_non_product_cacheable_selectors() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(
        &database,
        |_, _, inventory| {
            inventory.selectors = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true
                },
                {
                    "record_key": "pubkey",
                    "record_family": "pubkey",
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
                    "record_key": "pubkey",
                    "record_family": "pubkey",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection"
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        None,
    )
    .await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/records",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["record_cache"]["entries"],
        json!([
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
                "record_key": "pubkey",
                "record_family": "pubkey",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "record_family_not_supported_in_phase6_projection"
            }
        ])
    );
    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["addr:60".to_owned()]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_executes_verified_on_demand_without_persisting_cache_outcome(
) -> Result<()> {
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
            inventory.selectors = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
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
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
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
    let cache_outcome_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(&database.pool)
            .await
            .context("failed to count execution cache outcomes before diagnostics request")?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v2/diagnostics/names/Alice.eth/records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 diagnostics on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"]["comparison"]["addr:60"]["indexed"],
        json!({
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["comparison"]["addr:60"]["verified"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );
    assert_eq!(
        payload["data"]["value_sources"]["addr:60"][1],
        json!({
            "source": "verified",
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(rpc_requests[0]["method"], json!("eth_call"));
    assert_eq!(
        rpc_requests[0]["params"][1],
        json!({
            "blockHash": execution_block_hash,
            "requireCanonical": true
        })
    );

    let cache_outcome_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(&database.pool)
            .await
            .context("failed to count execution cache outcomes after diagnostics request")?;
    assert_eq!(
        cache_outcome_count_after, cache_outcome_count_before,
        "diagnostics records GET must not persist execution_cache_outcomes rows"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_bounds_on_demand_rpc_burst_for_keyed_comparison()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_block_hash =
        "0x2222222222222222222222222222222222222222222222222222222222222222";

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

    let executed_address = "0x0000000000000000000000000000000000000f0f";
    let (rpc_url, rpc_handle) =
        spawn_diagnostics_burst_observing_mock_rpc(5, executed_address).await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(
                    "/v2/diagnostics/names/Alice.eth/records?keys=addr:0,addr:1,addr:2,addr:3,addr:4",
                )
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 diagnostics bounded on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"]["comparison"]
            .as_object()
            .expect("comparison must be an object")
            .len(),
        5
    );

    let rpc_stats = join_diagnostics_burst_observing_mock_rpc(rpc_handle).await?;
    assert_eq!(rpc_stats.requests.len(), 5);
    assert_eq!(
        rpc_stats.max_in_flight, 4,
        "diagnostics records on-demand RPC burst must stay bounded to four concurrent selectors"
    );
    for request in rpc_stats.requests {
        assert_eq!(request["method"], json!("eth_call"));
        assert_eq!(
            request["params"][1],
            json!({
                "blockHash": execution_block_hash,
                "requireCanonical": true
            })
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_return_not_found_for_missing_name() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v2/diagnostics/names/missing.eth/{suffix}");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::NOT_FOUND).await?;

        assert_eq!(payload["error"]["code"], json!("not_found"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!("name missing.eth was not found in namespace ens"),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_honor_snapshot_selectors() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let snapshot_token =
        seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!(
            "/v2/diagnostics/names/alice.eth/{suffix}?at={snapshot_token}&finality=finalized"
        );
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xdiag1406f43",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_infer_basenames_namespace() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "basenames:alice.base.eth", 84).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v2/diagnostics/names/alice.base.eth/{suffix}");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["8453"],
            json!({
                "block_number": 84,
                "block_hash": "0xdiag54",
                "timestamp": "2026-04-17T00:00:24Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_honor_namespace_override() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.base.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v2/diagnostics/names/alice.base.eth/{suffix}?namespace=ens");
        let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;

        assert_eq!(
            payload["meta"]["as_of"]["1"],
            json!({
                "block_number": 21_000_003,
                "block_hash": "0xdiag1406f43",
                "timestamp": "2026-04-17T00:00:03Z"
            }),
            "{uri}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_malformed_name() -> Result<()> {
    let state = AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("name normalization rejection does not use the database"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    };

    for suffix in ["coverage", "binding", "authority", "records"] {
        let uri = format!("/v2/diagnostics/names/bad%20name.eth/{suffix}");
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 malformed diagnostic name request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_undocumented_query_params() -> Result<()> {
    let state = AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("query rejection does not use the database"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    };

    for suffix in ["coverage", "binding", "authority"] {
        for (query, expected_message) in [
            ("source=verified", "unknown query parameter: source"),
            ("keys=addr:60", "unknown query parameter: keys"),
            ("address=bad", "unknown query parameter: address"),
            ("page_size=201", "unknown query parameter: page_size"),
        ] {
            let uri = format!("/v2/diagnostics/names/alice.eth/{suffix}?{query}");
            let response = app_router(state.clone())
                .oneshot(
                    Request::builder()
                        .uri(&uri)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("v2 diagnostic name undocumented query request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let payload: Value = read_json(response).await?;
            assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
            assert_eq!(
                payload["error"]["message"],
                json!(expected_message),
                "{uri}"
            );
        }
    }

    for (query, expected_message) in [
        ("source=verified", "unknown query parameter: source"),
        ("address=bad", "unknown query parameter: address"),
        ("page_size=201", "unknown query parameter: page_size"),
    ] {
        let uri = format!("/v2/diagnostics/names/alice.eth/records?{query}");
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(&uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 diagnostic records undocumented query request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!(expected_message),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_records_rejects_malformed_duplicate_and_unknown_query_params()
-> Result<()> {
    let state = AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("query rejection does not use the database"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    };

    for (uri, expected_message) in [
        (
            "/v2/diagnostics/names/alice.eth/records?keys=bad%20key",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=abi",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=addr:060,addr:60",
            "keys must not contain duplicate record keys",
        ),
        (
            "/v2/diagnostics/names/alice.eth/records?keys=addr:60&source=verified",
            "unknown query parameter: source",
        ),
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 records diagnostic invalid keys request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!(expected_message),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_routes_reject_invalid_namespace_and_at() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;

    for suffix in ["coverage", "binding", "authority", "records"] {
        let invalid_namespace = format!("/v2/diagnostics/names/alice.eth/{suffix}?namespace=unknown");
        let payload =
            request_v2_diagnostics_json(&database, &invalid_namespace, StatusCode::BAD_REQUEST)
                .await?;
        assert_eq!(
            payload["error"]["code"],
            json!("invalid_input"),
            "{invalid_namespace}"
        );

        let invalid_at = format!("/v2/diagnostics/names/alice.eth/{suffix}?at=not-hex");
        let payload =
            request_v2_diagnostics_json(&database, &invalid_at, StatusCode::BAD_REQUEST).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{invalid_at}");
        assert_eq!(payload["error"]["message"], json!("at is invalid"), "{invalid_at}");
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_requires_keys() -> Result<()> {
    let state = AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("keys rejection does not use the database"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    };

    for uri in [
        "/v2/diagnostics/names/alice.eth/execution",
        "/v2/diagnostics/names/alice.eth/execution?keys=",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 execution diagnostic keys-required request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(
            payload["error"]["message"],
            json!("keys is required"),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_rejects_malformed_duplicate_and_unknown_query_params()
-> Result<()> {
    let state = AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("query rejection does not use the database"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    };

    for (uri, expected_message) in [
        (
            "/v2/diagnostics/names/alice.eth/execution?keys=bad%20key",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/execution?keys=abi",
            "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
        ),
        (
            "/v2/diagnostics/names/alice.eth/execution?keys=addr:060,addr:60",
            "keys must not contain duplicate record keys",
        ),
        (
            "/v2/diagnostics/names/alice.eth/execution?keys=addr:60&source=verified",
            "unknown query parameter: source",
        ),
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 execution diagnostic invalid query request failed")?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(payload["error"]["message"], json!(expected_message), "{uri}");
    }

    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_returns_not_found_when_persisted_artifact_is_missing()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_diagnostics_execution_name(&database, false).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::NOT_FOUND,
    )
    .await?;

    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_ignores_mismatched_cache_boundaries() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002021);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let verified_queries = v2_execution_verified_queries(
        execution_trace_id,
        "0x00000000000000000000000000000000000000aa",
    );
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        verified_queries.clone(),
    );
    let mut outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        verified_queries,
        &logical_name_id,
        resource_id,
    );
    outcome.cache_key.manifest_versions = json!([{
        "manifest_version": 99,
        "source_family": "ens_v1_registry",
        "chain": "ethereum-mainnet",
        "deployment_epoch": "ens_v1"
    }]);
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::NOT_FOUND,
    )
    .await?;

    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_returns_stale_for_stale_inventory_join() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002022);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let verified_queries = v2_execution_verified_queries(
        execution_trace_id,
        "0x00000000000000000000000000000000000000aa",
    );
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        verified_queries,
        &logical_name_id,
        resource_id,
    );
    let mut stale_inventory = record_inventory_current_row(&logical_name_id, resource_id);
    stale_inventory.chain_positions = v2_execution_chain_positions(21_000_004, "0xbinding004");

    database
        .insert_record_inventory_current_row(stale_inventory)
        .await?;
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::CONFLICT,
    )
    .await?;

    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_returns_persisted_explain_shape() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002001);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let verified_queries = v2_execution_verified_queries(
        execution_trace_id,
        "0x00000000000000000000000000000000000000aa",
    );
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        verified_queries.clone(),
        &logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::OK,
    )
    .await?;

    assert!(payload.get("page").is_none());
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );
    assert!(
        payload["data"].get("declared_state").is_none(),
        "v2 diagnostics execution data must not use the v1 declared_state wrapper"
    );
    assert!(
        payload["data"].get("verified_state").is_none(),
        "v2 diagnostics execution data must not use the v1 verified_state wrapper"
    );
    assert_eq!(
        payload["data"]["execution_trace_id"],
        json!(execution_trace_id.to_string())
    );
    let mut expected_verified_queries = verified_queries.clone();
    expected_verified_queries[0]["status"] = json!("ok");
    assert_eq!(
        payload["data"]["verified_queries"],
        expected_verified_queries
    );
    assert_eq!(
        payload["data"]["steps"][0]["step_kind"],
        json!("load_declared_topology")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_falls_back_to_full_avatar_selector_artifact() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002041);
    let request_key = resolution_execution_request_key(&["avatar", "text:com.twitter"]);
    let verified_queries = v2_avatar_text_execution_verified_queries(execution_trace_id);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter"],
        verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        verified_queries.clone(),
        &logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=avatar,text:com.twitter",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["execution_trace_id"],
        json!(execution_trace_id.to_string())
    );
    assert_eq!(
        payload["data"]["verified_queries"][0]["record_key"],
        json!("avatar")
    );
    assert_eq!(
        payload["data"]["verified_queries"][1]["record_key"],
        json!("text:com.twitter")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_returns_not_found_for_partial_compact_avatar_hit(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002045);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let verified_queries = v2_text_execution_verified_queries(execution_trace_id);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter"],
        verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        verified_queries,
        &logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=avatar,text:com.twitter",
        StatusCode::NOT_FOUND,
    )
    .await?;

    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_selects_basenames_cross_chain_artifact() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, boundary) =
        seed_v2_diagnostics_basenames_execution_name(&database).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002031);
    let request_key = resolution_execution_request_key_for_name(&logical_name_id, &["addr:60"]);
    let verified_queries = v2_execution_verified_queries(
        execution_trace_id,
        "0x00000000000000000000000000000000000000bb",
    );
    let base_only_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002032);
    let base_only_queries = v2_execution_verified_queries(
        base_only_trace_id,
        "0x00000000000000000000000000000000000000cc",
    );
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.chain_context["requested_positions"] = v2_basenames_execution_requested_positions();
    trace.manifest_context["manifest_versions"] = basenames_execution_manifest_versions();
    trace.request_metadata["surface"] = json!("alice.base.eth");

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        verified_queries,
        boundary.clone(),
        boundary.clone(),
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.manifest_versions = basenames_execution_manifest_versions();
    outcome.cache_key.requested_chain_positions = v2_basenames_execution_requested_positions();
    let mut base_only_trace = resolution_execution_trace(
        base_only_trace_id,
        &request_key,
        &["addr:60"],
        base_only_queries.clone(),
    );
    base_only_trace.namespace = "basenames".to_owned();
    base_only_trace.chain_context["requested_positions"] =
        v2_basenames_base_only_execution_requested_positions();
    base_only_trace.manifest_context["manifest_versions"] = basenames_execution_manifest_versions();
    base_only_trace.request_metadata["surface"] = json!("alice.base.eth");
    base_only_trace.finished_at = Some(timestamp(1_717_172_900));
    let mut base_only_outcome = resolution_execution_outcome_with_boundaries(
        base_only_trace_id,
        &request_key,
        base_only_queries,
        boundary.clone(),
        boundary.clone(),
    );
    base_only_outcome.namespace = "basenames".to_owned();
    base_only_outcome.cache_key.manifest_versions = basenames_execution_manifest_versions();
    base_only_outcome.cache_key.requested_chain_positions =
        v2_basenames_base_only_execution_requested_positions();
    base_only_outcome.finished_at = timestamp(1_717_172_900);

    upsert_execution_trace(&database.pool, &base_only_trace).await?;
    upsert_execution_outcome(&database.pool, &base_only_outcome).await?;
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.base.eth/execution?keys=addr:60",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["execution_trace_id"],
        json!(execution_trace_id.to_string())
    );
    assert_eq!(
        payload["meta"]["as_of"]["8453"],
        json!({
            "block_number": 84,
            "block_hash": "0xbase54",
            "timestamp": "2026-04-17T00:00:24Z"
        })
    );
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_accepts_basenames_base_only_inventory_for_cross_chain_snapshot()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, boundary) =
        seed_v2_diagnostics_basenames_execution_name(&database).await?;
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002044);
    let request_key = resolution_execution_request_key_for_name(&logical_name_id, &["addr:60"]);
    let verified_queries = v2_execution_verified_queries(
        execution_trace_id,
        "0x00000000000000000000000000000000000000dd",
    );
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.chain_context["requested_positions"] = v2_basenames_execution_requested_positions();
    trace.manifest_context["manifest_versions"] = basenames_execution_manifest_versions();
    trace.request_metadata["surface"] = json!("alice.base.eth");

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        verified_queries,
        boundary.clone(),
        boundary,
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.manifest_versions = basenames_execution_manifest_versions();
    outcome.cache_key.requested_chain_positions = v2_basenames_execution_requested_positions();

    let mut base_only_inventory = record_inventory_current_row(
        &logical_name_id,
        Uuid::from_u128(0x2231),
    );
    base_only_inventory.record_version_boundary = outcome.cache_key.record_version_boundary.clone();
    base_only_inventory.chain_positions = v2_basenames_base_only_inventory_chain_positions();
    base_only_inventory.provenance = json!({
        "manifest_versions": basenames_execution_manifest_versions()
    });
    base_only_inventory.coverage = json!({
        "status": "partial",
        "unsupported_reason": null
    });
    database
        .insert_record_inventory_current_row(base_only_inventory)
        .await?;

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.base.eth/execution?keys=addr:60",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["execution_trace_id"],
        json!(execution_trace_id.to_string())
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_selects_artifact_at_or_before_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (logical_name_id, resource_id, older_snapshot_token) =
        seed_v2_diagnostics_execution_name(&database, true).await?;
    let later_positions = v2_execution_chain_positions(21_000_006, "0xbinding006");
    database
        .seed_snapshot_selector_chain_positions(&later_positions)
        .await?;
    seed_v2_execution_lineage_path(
        &database,
        &[
            (21_000_003, "0xbinding"),
            (21_000_004, "0xbinding004"),
            (21_000_005, "0xbinding005"),
            (21_000_006, "0xbinding006"),
        ],
    )
    .await?;

    let request_key = resolution_execution_request_key(&["addr:60"]);
    let older_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002010);
    let later_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002011);
    let older_queries = v2_execution_verified_queries(
        older_trace_id,
        "0x00000000000000000000000000000000000000aa",
    );
    let later_queries = v2_execution_verified_queries(
        later_trace_id,
        "0x00000000000000000000000000000000000000bb",
    );
    let older_trace = resolution_execution_trace(
        older_trace_id,
        &request_key,
        &["addr:60"],
        older_queries.clone(),
    );
    let older_outcome = resolution_execution_outcome(
        older_trace_id,
        &request_key,
        older_queries,
        &logical_name_id,
        resource_id,
    );
    let mut later_trace = resolution_execution_trace(
        later_trace_id,
        &request_key,
        &["addr:60"],
        later_queries,
    );
    let mut later_outcome = resolution_execution_outcome(
        later_trace_id,
        &request_key,
        v2_execution_verified_queries(
            later_trace_id,
            "0x00000000000000000000000000000000000000bb",
        ),
        &logical_name_id,
        resource_id,
    );
    set_resolution_execution_artifact_position(
        &mut later_trace,
        &mut later_outcome,
        21_000_006,
        "0xbinding006",
    );
    later_trace.finished_at = Some(timestamp(1_717_172_100));
    later_outcome.finished_at = timestamp(1_717_172_100);

    upsert_execution_trace(&database.pool, &older_trace).await?;
    upsert_execution_outcome(&database.pool, &older_outcome).await?;
    upsert_execution_trace(&database.pool, &later_trace).await?;
    upsert_execution_outcome(&database.pool, &later_outcome).await?;

    let older_payload = request_v2_diagnostics_json(
        &database,
        &format!(
            "/v2/diagnostics/names/alice.eth/execution?keys=addr:60&at={older_snapshot_token}&finality=finalized"
        ),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(
        older_payload["data"]["execution_trace_id"],
        json!(older_trace_id.to_string())
    );
    assert_eq!(
        older_payload["meta"]["as_of"]["1"]["block_number"],
        json!(21_000_003)
    );

    let latest_payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::OK,
    )
    .await?;
    assert_eq!(
        latest_payload["data"]["execution_trace_id"],
        json!(later_trace_id.to_string())
    );
    assert_eq!(
        latest_payload["meta"]["as_of"]["1"]["block_number"],
        json!(21_000_006)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_skips_forked_older_artifact() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    seed_v2_execution_lineage_path(
        &database,
        &[(21_000_002, "0xbinding002"), (21_000_003, "0xbinding")],
    )
    .await?;
    let fork_hash = "0x00000000000000000000000000000000000000000000000000000000feed0002";
    seed_v2_execution_lineage_block(
        &database,
        "ethereum-mainnet",
        21_000_002,
        fork_hash,
        Some("0xbinding001"),
        "orphaned",
    )
    .await?;

    let request_key = resolution_execution_request_key(&["addr:60"]);
    let canonical_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002050);
    let fork_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002051);
    let canonical_queries = v2_execution_verified_queries(
        canonical_trace_id,
        "0x00000000000000000000000000000000000000aa",
    );
    let fork_queries = v2_execution_verified_queries(
        fork_trace_id,
        "0x00000000000000000000000000000000000000ff",
    );
    let mut canonical_trace = resolution_execution_trace(
        canonical_trace_id,
        &request_key,
        &["addr:60"],
        canonical_queries.clone(),
    );
    let mut canonical_outcome = resolution_execution_outcome(
        canonical_trace_id,
        &request_key,
        canonical_queries,
        &logical_name_id,
        resource_id,
    );
    set_resolution_execution_artifact_position(
        &mut canonical_trace,
        &mut canonical_outcome,
        21_000_002,
        "0xbinding002",
    );
    canonical_trace.finished_at = Some(timestamp(1_717_172_000));
    canonical_outcome.finished_at = timestamp(1_717_172_000);

    let mut fork_trace =
        resolution_execution_trace(fork_trace_id, &request_key, &["addr:60"], fork_queries.clone());
    let mut fork_outcome = resolution_execution_outcome(
        fork_trace_id,
        &request_key,
        fork_queries,
        &logical_name_id,
        resource_id,
    );
    set_resolution_execution_artifact_position(
        &mut fork_trace,
        &mut fork_outcome,
        21_000_002,
        fork_hash,
    );
    fork_trace.finished_at = Some(timestamp(1_717_172_100));
    fork_outcome.finished_at = timestamp(1_717_172_100);

    upsert_execution_trace(&database.pool, &canonical_trace).await?;
    upsert_execution_outcome(&database.pool, &canonical_outcome).await?;
    upsert_execution_trace(&database.pool, &fork_trace).await?;
    upsert_execution_outcome(&database.pool, &fork_outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::OK,
    )
    .await?;

    assert_eq!(
        payload["data"]["execution_trace_id"],
        json!(canonical_trace_id.to_string())
    );
    assert_eq!(
        payload["data"]["verified_queries"][0]["value"]["value"],
        json!("0x00000000000000000000000000000000000000aa")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_diagnostics_name_execution_returns_not_found_for_only_forked_older_artifact()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let (logical_name_id, resource_id, _) =
        seed_v2_diagnostics_execution_name(&database, false).await?;
    seed_v2_execution_lineage_path(
        &database,
        &[(21_000_002, "0xbinding002"), (21_000_003, "0xbinding")],
    )
    .await?;
    let fork_hash = "0x00000000000000000000000000000000000000000000000000000000feed0003";
    seed_v2_execution_lineage_block(
        &database,
        "ethereum-mainnet",
        21_000_002,
        fork_hash,
        Some("0xbinding001"),
        "orphaned",
    )
    .await?;

    let request_key = resolution_execution_request_key(&["addr:60"]);
    let fork_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002052);
    let fork_queries = v2_execution_verified_queries(
        fork_trace_id,
        "0x00000000000000000000000000000000000000ff",
    );
    let mut fork_trace =
        resolution_execution_trace(fork_trace_id, &request_key, &["addr:60"], fork_queries.clone());
    let mut fork_outcome = resolution_execution_outcome(
        fork_trace_id,
        &request_key,
        fork_queries,
        &logical_name_id,
        resource_id,
    );
    set_resolution_execution_artifact_position(
        &mut fork_trace,
        &mut fork_outcome,
        21_000_002,
        fork_hash,
    );
    upsert_execution_trace(&database.pool, &fork_trace).await?;
    upsert_execution_outcome(&database.pool, &fork_outcome).await?;

    let payload = request_v2_diagnostics_json(
        &database,
        "/v2/diagnostics/names/alice.eth/execution?keys=addr:60",
        StatusCode::NOT_FOUND,
    )
    .await?;

    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

async fn request_v2_diagnostics_json(
    database: &TestDatabase,
    uri: &str,
    expected_status: StatusCode,
) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("v2 diagnostics name request failed for {uri}"))?;
    let status = response.status();
    let payload = read_json(response).await?;

    assert_eq!(status, expected_status, "{uri}: {payload}");
    Ok(payload)
}

struct DiagnosticsBurstRpcStats {
    requests: Vec<Value>,
    max_in_flight: usize,
}

async fn spawn_diagnostics_burst_observing_mock_rpc(
    request_count: usize,
    address: &str,
) -> Result<(
    String,
    tokio::task::JoinHandle<Result<DiagnosticsBurstRpcStats>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind diagnostics burst mock RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let response = resolution_universal_resolver_addr60_response(address);
    let handle = tokio::spawn(async move {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..request_count {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept diagnostics burst mock RPC request")?;
            let requests = Arc::clone(&requests);
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            let response = response.clone();
            tasks.push(tokio::spawn(async move {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                update_diagnostics_burst_max(&max_in_flight, current);
                let result = async {
                    let request_payload = read_primary_name_mock_rpc_request(&mut socket).await?;
                    requests.lock().await.push(request_payload);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    write_primary_name_mock_rpc_response(&mut socket, response).await
                }
                .await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                result
            }));
        }

        for task in tasks {
            task.await
                .context("diagnostics burst mock RPC task panicked or was cancelled")??;
        }

        Ok(DiagnosticsBurstRpcStats {
            requests: std::mem::take(&mut *requests.lock().await),
            max_in_flight: max_in_flight.load(Ordering::SeqCst),
        })
    });

    Ok((url, handle))
}

fn update_diagnostics_burst_max(max_in_flight: &std::sync::atomic::AtomicUsize, current: usize) {
    use std::sync::atomic::Ordering;

    let mut observed = max_in_flight.load(Ordering::Relaxed);
    while current > observed {
        match max_in_flight.compare_exchange(
            observed,
            current,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next_observed) => observed = next_observed,
        }
    }
}

async fn join_diagnostics_burst_observing_mock_rpc(
    handle: tokio::task::JoinHandle<Result<DiagnosticsBurstRpcStats>>,
) -> Result<DiagnosticsBurstRpcStats> {
    handle
        .await
        .context("diagnostics burst mock RPC task panicked or was cancelled")?
}

async fn seed_v2_diagnostics_execution_name(
    database: &TestDatabase,
    migrated_schema: bool,
) -> Result<(String, Uuid, String)> {
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    if migrated_schema {
        database
            .seed_name_current_binding_migrated(
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
    } else {
        database
            .seed_name_current_binding(
                logical_name_id,
                "ens",
                "alice.eth",
                "alice.eth",
                "namehash:alice.eth",
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
    }
    let row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    let snapshot_token = hex::encode(
        serde_json::to_vec(&row.chain_positions).expect("chain positions must serialize"),
    );
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    Ok((logical_name_id.to_owned(), resource_id, snapshot_token))
}

async fn seed_v2_diagnostics_basenames_execution_name(
    database: &TestDatabase,
) -> Result<(String, Value)> {
    let logical_name_id = "basenames:alice.base.eth";
    let normalized_name = "alice.base.eth";
    let resource_id = Uuid::from_u128(0x2231);
    let token_lineage_id = Uuid::from_u128(0x1131);
    let surface_binding_id = Uuid::from_u128(0x3331);
    let chain_positions = v2_basenames_execution_chain_positions();

    database
        .seed_name_current_binding(
            logical_name_id,
            "basenames",
            normalized_name,
            normalized_name,
            &format!("namehash:{normalized_name}"),
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let row = bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "basenames".to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "resolver": {
                "chain_id": "base-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        }),
        provenance: json!({
            "manifest_versions": basenames_execution_manifest_versions()
        }),
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["basenames_execution"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        }),
        chain_positions: chain_positions.clone(),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "base-mainnet": "finalized",
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 2,
        last_recomputed_at: timestamp(1_717_176_084),
    };
    let (_, boundary) = bigname_storage::resolution_record_inventory_lookup_key(&row)
        .expect("basenames execution row must provide an inventory boundary");
    database.insert_name_current_row(row).await?;

    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.record_version_boundary = boundary.clone();
    inventory.chain_positions = chain_positions;
    inventory.provenance = json!({
        "manifest_versions": basenames_execution_manifest_versions()
    });
    inventory.coverage = json!({
        "status": "partial",
        "unsupported_reason": null
    });
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    Ok((logical_name_id.to_owned(), boundary))
}

fn resolution_execution_request_key_for_name(logical_name_id: &str, records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("{logical_name_id}:{}", records.join(","))
}

fn v2_execution_verified_queries(execution_trace_id: Uuid, address: &str) -> Value {
    json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": address
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ])
}

fn v2_avatar_text_execution_verified_queries(execution_trace_id: Uuid) -> Value {
    json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "uri": "https://example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ])
}

fn v2_text_execution_verified_queries(execution_trace_id: Uuid) -> Value {
    json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "key": "com.twitter",
                "value": "compact-alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ])
}

fn diagnostic_text_record_selectors(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "record_key": format!("text:key{index:02}"),
                "record_family": "text",
                "selector_key": format!("key{index:02}"),
                "cacheable": true
            })
        })
        .collect()
}

fn diagnostic_text_record_entries(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "record_key": format!("text:key{index:02}"),
                "record_family": "text",
                "selector_key": format!("key{index:02}"),
                "status": "success",
                "value": {
                    "key": format!("key{index:02}"),
                    "value": format!("value-{index:02}")
                }
            })
        })
        .collect()
}

fn diagnostic_text_record_keys(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("text:key{index:02}"))
        .collect()
}

fn diagnostic_text_verified_queries(count: usize, execution_trace_id: Uuid) -> Value {
    Value::Array(
        (0..count)
            .map(|index| {
                json!({
                    "record_key": format!("text:key{index:02}"),
                    "status": "success",
                    "value": {
                        "key": format!("key{index:02}"),
                        "value": format!("verified-{index:02}")
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                })
            })
            .collect(),
    )
}

fn v2_execution_chain_positions(block_number: i64, block_hash: &str) -> Value {
    json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
        }
    })
}

async fn seed_v2_execution_lineage_path(
    database: &TestDatabase,
    blocks: &[(i64, &str)],
) -> Result<()> {
    let mut parent_hash = None;
    for (block_number, block_hash) in blocks {
        seed_v2_execution_lineage_block(
            database,
            "ethereum-mainnet",
            *block_number,
            block_hash,
            parent_hash,
            "finalized",
        )
        .await?;
        parent_hash = Some(*block_hash);
    }
    Ok(())
}

async fn seed_v2_execution_lineage_block(
    database: &TestDatabase,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    parent_hash: Option<&str>,
    canonicality_state: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5::timestamptz, $6::canonicality_state)
        ON CONFLICT (chain_id, block_hash) DO UPDATE SET
            parent_hash = EXCLUDED.parent_hash,
            block_number = EXCLUDED.block_number,
            block_timestamp = EXCLUDED.block_timestamp,
            canonicality_state = EXCLUDED.canonicality_state
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(parent_hash)
    .bind(block_number)
    .bind(format!(
        "2026-04-17T00:00:{:02}Z",
        block_number.rem_euclid(60)
    ))
    .bind(canonicality_state)
    .execute(&database.pool)
    .await
    .with_context(|| {
        format!(
            "failed to seed v2 execution lineage block {chain_id} {block_hash} at {block_number}"
        )
    })?;

    Ok(())
}

fn v2_basenames_execution_chain_positions() -> Value {
    json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 84,
            "block_hash": "0xbase54",
            "timestamp": "2026-04-17T00:00:24Z"
        },
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    })
}

fn v2_basenames_execution_requested_positions() -> Value {
    json!([
        {
            "chain_id": "base-mainnet",
            "block_number": 84,
            "block_hash": "0xbase54"
        },
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding"
        }
    ])
}

fn v2_basenames_base_only_execution_requested_positions() -> Value {
    json!([{
        "chain_id": "base-mainnet",
        "block_number": 84,
        "block_hash": "0xbase54"
    }])
}

fn v2_basenames_base_only_inventory_chain_positions() -> Value {
    json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 84,
            "block_hash": "0xbase54",
            "timestamp": "2026-04-17T00:00:24Z"
        }
    })
}

fn basenames_execution_manifest_versions() -> Value {
    json!([
        {
            "source_family": "basenames_execution",
            "manifest_version": 2,
            "chain": "ethereum-mainnet",
            "deployment_epoch": "basenames_v1"
        }
    ])
}

fn v2_execution_requested_positions(block_number: i64, block_hash: &str) -> Value {
    json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": block_number,
        "block_hash": block_hash
    }])
}

fn set_resolution_execution_artifact_position(
    trace: &mut ExecutionTrace,
    outcome: &mut ExecutionOutcome,
    block_number: i64,
    block_hash: &str,
) {
    let requested_positions = v2_execution_requested_positions(block_number, block_hash);
    trace.chain_context["requested_positions"] = requested_positions.clone();
    outcome.cache_key.requested_chain_positions = requested_positions;
    for step in &mut trace.steps {
        step.canonicality_dependency = json!({
            "ethereum-mainnet": {
                "block_hash": block_hash,
                "block_number": block_number,
                "state": "finalized"
            }
        });
    }
}

async fn seed_v2_diagnostics_name_fixture(
    database: &TestDatabase,
    logical_name_id: &str,
    block_number: i64,
) -> Result<String> {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            namespace,
            normalized_name,
            normalized_name,
            &format!("namehash:{normalized_name}"),
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;

    let row = diagnostic_name_current_row(
        logical_name_id,
        block_number,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    );
    let snapshot_token = hex::encode(
        serde_json::to_vec(&row.chain_positions).expect("chain positions must serialize"),
    );
    database.insert_name_current_row(row).await?;

    Ok(snapshot_token)
}

fn diagnostic_name_current_row(
    logical_name_id: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);
    let block_hash = format!("0xdiag{block_number:x}");

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "control": {
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "NameTransferred"
            },
            "history": {
                "latest_event_kind": "NameTransferred"
            }
        }),
        provenance: json!({}),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [source_family_for_namespace(namespace)],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
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
