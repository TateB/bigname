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
        worker_boundary
    );
    assert_eq!(
        payload["data"]["record_cache"]["record_version_boundary"],
        worker_boundary
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

    for suffix in ["coverage", "binding", "authority", "records"] {
        for query in ["source=verified", "keys=addr:60", "address=bad", "page_size=201"] {
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
                json!("query parameters are invalid"),
                "{uri}"
            );
        }
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

    assert_eq!(response.status(), expected_status, "{uri}");
    read_json(response).await
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
