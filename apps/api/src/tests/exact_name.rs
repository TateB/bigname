#[tokio::test]
async fn get_name_returns_current_projection_envelope() -> Result<()> {
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(payload.consistency, "head");
    assert_eq!(payload.last_updated, "2024-05-31T16:08:37Z");

    let data = payload.data.as_object().expect("data must be an object");
    assert_eq!(
        data.get("logical_name_id"),
        Some(&Value::String("ens:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namespace"),
        Some(&Value::String("ens".to_owned()))
    );
    assert_eq!(
        data.get("normalized_name"),
        Some(&Value::String("alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("canonical_display_name"),
        Some(&Value::String("Alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namehash"),
        Some(&Value::String("namehash:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        data.get("token_lineage_id"),
        Some(&Value::String(token_lineage_id.to_string()))
    );
    assert_eq!(
        data.get("binding_kind"),
        Some(&Value::String("declared_registry_path".to_owned()))
    );

    let declared_state = payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");
    assert_eq!(
        declared_state
            .get("registration")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("active")
    );
    assert_eq!(
        declared_state
            .get("resolver")
            .and_then(Value::as_object)
            .and_then(|value| value.get("address"))
            .and_then(Value::as_str),
        Some("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        declared_state
            .get("authority")
            .and_then(Value::as_object)
            .and_then(|value| value.get("resource_id")),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        declared_state.get("record_inventory").cloned(),
        Some(json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "enumeration_basis": {
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false
            },
            "selectors": [
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
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "cacheable": false
                }
            ],
            "explicit_gaps": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ],
            "unsupported_families": [
                {
                    "record_family": "abi",
                    "unsupported_reason": "resolver_family_pending"
                },
                {
                    "record_family": "pubkey",
                    "unsupported_reason": "resolver_family_pending"
                }
            ],
            "last_change": {
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xlastchange",
                    "timestamp": "2026-04-17T00:00:04Z"
                }
            }
        }))
    );
    assert_eq!(
        declared_state
            .get("history")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );

    assert!(payload.provenance.is_null());

    let coverage = payload
        .coverage
        .as_object()
        .expect("coverage must be an object");
    assert_eq!(coverage.get("status").and_then(Value::as_str), Some("full"));
    assert_eq!(
        coverage.get("exhaustiveness").and_then(Value::as_str),
        Some("authoritative")
    );
    assert_eq!(
        coverage.get("source_classes_considered"),
        Some(&json!(["ensv1_registry_path"]))
    );
    assert_eq!(
        coverage.get("enumeration_basis").and_then(Value::as_str),
        Some("exact_name")
    );
    assert_eq!(coverage.get("unsupported_reason"), Some(&Value::Null));

    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn exact_name_routes_reject_invalid_snapshot_selectors_as_invalid_input() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let valid_positions = encode_query_value(
        r#"{"ethereum":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"}}"#,
    );
    let duplicate_slot_positions = encode_query_value(
        r#"{"ethereum":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"},"ethereum":{"chain_id":"ethereum-mainnet","block_number":21000004,"block_hash":"0xduplicate","timestamp":"2026-04-17T00:00:04Z"}}"#,
    );
    let unsupported_slot_positions = encode_query_value(
        r#"{"polygon":{"chain_id":"ethereum-mainnet","block_number":21000003,"block_hash":"0xbinding","timestamp":"2026-04-17T00:00:03Z"}}"#,
    );
    let cases = vec![
        (
            "at plus chain_positions",
            format!("at=2026-04-17T00%3A00%3A03Z&chain_positions={valid_positions}"),
            "at and chain_positions are mutually exclusive snapshot selectors",
        ),
        (
            "malformed chain_positions",
            "chain_positions=%7B".to_owned(),
            "chain_positions must be one JSON object",
        ),
        (
            "duplicate position slot",
            format!("chain_positions={duplicate_slot_positions}"),
            "chain_positions repeats position slot ethereum",
        ),
        (
            "unsupported position slot",
            format!("chain_positions={unsupported_slot_positions}"),
            "unsupported snapshot position slot polygon",
        ),
    ];
    let routes = [
        "/v1/names/ens/alice.eth",
        "/v1/coverage/ens/alice.eth",
        "/v1/explain/names/ens/alice.eth/surface-binding",
        "/v1/explain/names/ens/alice.eth/authority-control",
    ];

    for route in routes {
        for (label, query, expected_message_fragment) in &cases {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("{route}?{query}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!("{label} exact-name selector request failed for {route}")
                })?;

            assert_public_invalid_input_response(response, expected_message_fragment).await?;
        }
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
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

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_unsupported_record_inventory_when_projection_row_is_missing() -> Result<()>
{
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
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("unsupported_reason"))
            .and_then(Value::as_str),
        Some("declared record inventory summary is not yet projected")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_declared_state_explain_with_shared_top_level_coverage() -> Result<()>
{
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

    let coverage_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("coverage request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(coverage_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let coverage_payload: NameResponse = read_json(coverage_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(coverage_payload.provenance.is_object());
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(coverage_payload.consistency, name_payload.consistency);
    assert_eq!(coverage_payload.last_updated, name_payload.last_updated);
    assert_eq!(coverage_payload.verified_state, None);
    assert_eq!(
        coverage_payload.declared_state,
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_surface_binding_explain_reuses_exact_name_envelope_fields() -> Result<()> {
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
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        },
        "history": {
            "surface_head": null,
            "resource_head": null
        }
    });
    database.insert_name_current_row(row).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/alice.eth/surface-binding")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("surface-binding explain request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(explain_payload.provenance.is_object());
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("history"),
        name_payload.declared_state.get("history")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "surface_binding": {
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "history": {
                "surface_head": null,
                "resource_head": null
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_authority_control_explain_reuses_exact_name_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let registrant = "0x0000000000000000000000000000000000000abc";
    let registry_owner = "0x0000000000000000000000000000000000000def";

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
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "control": {
            "registrant": registrant,
            "registry_owner": registry_owner,
            "latest_event_kind": "NameWrapped"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        }
    });
    database.insert_name_current_row(row).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/ens/alice.eth/authority-control")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("authority-control explain request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(explain_payload.provenance.is_object());
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("authority"),
        name_payload.declared_state.get("authority")
    );
    assert_eq!(
        explain_payload.declared_state.get("control"),
        name_payload.declared_state.get("control")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "authority": {
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "control": {
                "registrant": registrant,
                "registry_owner": registry_owner,
                "latest_event_kind": "NameWrapped"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_reads_rebuilt_basenames_exact_name_projection() -> Result<()> {
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
                .uri("/v1/names/basenames/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames exact-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    let history = payload
        .declared_state
        .get("history")
        .cloned()
        .expect("history summary must be present");
    assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(payload.data["namespace"], json!("basenames"));
    assert_eq!(
        payload.data["binding_kind"],
        json!("declared_registry_path")
    );
    assert_eq!(
        payload.declared_state.get("control"),
        Some(&basenames_exact_name_control_summary())
    );
    assert_eq!(
        payload.declared_state.get("resolver"),
        Some(&basenames_exact_name_resolver_summary())
    );
    assert_eq!(
        history
            .get("surface_head")
            .and_then(|value| value.get("event_kind")),
        Some(&json!("ResolverChanged"))
    );
    assert_eq!(
        history
            .get("resource_head")
            .and_then(|value| value.get("event_kind")),
        Some(&json!("ResolverChanged"))
    );
    assert_eq!(payload.coverage["status"], json!("full"));
    assert_eq!(
        payload.coverage["source_classes_considered"],
        json!(["ensv1_registry_path"])
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_reads_selected_sepolia_dev_ensv2_exact_name_profile_projection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:bob.alice.eth";
    let resource_id = Uuid::from_u128(0x8c10);
    let token_lineage_id = Uuid::from_u128(0x8c11);
    let surface_binding_id = Uuid::from_u128(0x8c12);
    let registrant = "0x0000000000000000000000000000000000000b0b";
    let controller = "0x0000000000000000000000000000000000000c0c";
    let (registry_manifest_id, registrar_manifest_id) =
        seed_ensv2_exact_name_profile_manifests(&database).await?;

    database
        .seed_ensv2_address_names_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
            registrant,
            controller,
        )
        .await?;
    seed_ensv2_exact_name_profile_registrar_event(
        &database,
        logical_name_id,
        resource_id,
        registrar_manifest_id,
    )
    .await?;
    assign_ensv2_exact_name_profile_source_manifests(
        &database,
        logical_name_id,
        registry_manifest_id,
        registrar_manifest_id,
    )
    .await?;
    database.rebuild_name_current(logical_name_id).await?;

    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/bob.alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 sepolia-dev exact-name request failed")?;
    let coverage_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/bob.alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 sepolia-dev coverage request failed")?;

    assert_eq!(name_response.status(), StatusCode::OK);
    assert_eq!(coverage_response.status(), StatusCode::OK);

    let name_payload: NameResponse = read_json(name_response).await?;
    let coverage_payload: NameResponse = read_json(coverage_response).await?;
    let supported_coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["ens_v2_registry_l1", "ens_v2_registrar_l1"],
        "unsupported_reason": null,
        "enumeration_basis": "exact_name_profile",
    });

    assert_eq!(name_payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(name_payload.data["namespace"], json!("ens"));
    assert_eq!(
        name_payload.data["binding_kind"],
        json!("linked_subregistry_path")
    );
    assert_eq!(
        name_payload.declared_state["registration"]["authority_kind"],
        json!("ens_v2_registry")
    );
    assert_eq!(
        name_payload.declared_state["registration"]["latest_event_kind"],
        json!("RegistrationRenewed")
    );
    assert_eq!(
        name_payload.declared_state["control"]["registry_owner"],
        json!(controller)
    );
    assert_eq!(name_payload.coverage, supported_coverage);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert_eq!(coverage_payload.declared_state, supported_coverage);
    assert_eq!(
        name_payload.chain_positions,
        json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 206,
                "block_hash": "0xensv2-regen",
                "timestamp": "2024-05-31T19:03:26Z"
            }
        })
    );
    assert_eq!(name_payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_reads_rebuilt_basenames_exact_name_control_vectors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let cases = [
        (
            "nft-only.base.eth",
            BasenamesControlVectorScenario::NftOnly,
            0x9260_u128,
        ),
        (
            "management-only.base.eth",
            BasenamesControlVectorScenario::ManagementOnly,
            0x9270_u128,
        ),
        (
            "full-transfer.base.eth",
            BasenamesControlVectorScenario::FullTransfer,
            0x9280_u128,
        ),
    ];

    for (name, scenario, base_id) in cases {
        let logical_name_id = format!("basenames:{name}");
        database
            .seed_basenames_control_vector_rebuild_inputs(
                &logical_name_id,
                Uuid::from_u128(base_id),
                Uuid::from_u128(base_id + 1),
                Uuid::from_u128(base_id + 2),
                scenario,
            )
            .await?;
        database.rebuild_name_current(&logical_name_id).await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/names/basenames/{name}"))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("Basenames exact-name request failed for {name}"))?;

        assert_eq!(response.status(), StatusCode::OK);

        let payload: NameResponse = read_json(response).await?;
        assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
        assert_eq!(
            payload.declared_state.get("control"),
            Some(&basenames_control_vector_control_summary(scenario))
        );
        assert_eq!(
            payload.declared_state.get("resolver"),
            Some(&basenames_exact_name_resolver_summary())
        );
        assert_eq!(payload.coverage["status"], json!("full"));
        assert_eq!(payload.verified_state, None);
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_reads_basenames_row_with_auxiliary_chain_position() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9285);
    let token_lineage_id = Uuid::from_u128(0x9286);
    let surface_binding_id = Uuid::from_u128(0x9287);

    database
        .seed_basenames_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xbasenamesl1",
            None,
            21_000_100,
            1_717_171_703,
        )],
    )
    .await
    .context("failed to upsert auxiliary ethereum block for Basenames exact-name test")?;
    database.rebuild_name_current(logical_name_id).await?;
    let mut row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("rebuilt Basenames exact-name row must exist")?;
    insert_basenames_supported_ethereum_position(&mut row);
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/basenames/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("Basenames exact-name request with auxiliary chain position failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: NameResponse = read_json(response).await?;
    assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(payload.data["namespace"], json!("basenames"));
    assert_eq!(payload.coverage["status"], json!("full"));
    assert_eq!(
        payload.chain_positions,
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 103,
                "block_hash": "0xbase-resolver",
                "timestamp": "2024-05-31T16:08:23Z"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_reads_shared_basenames_exact_name_coverage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9210);
    let token_lineage_id = Uuid::from_u128(0x9211);
    let surface_binding_id = Uuid::from_u128(0x9212);

    database
        .seed_basenames_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;

    let coverage_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/basenames/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames coverage request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/basenames/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames name request failed")?;

    assert_eq!(coverage_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let coverage_payload: NameResponse = read_json(coverage_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(coverage_payload.provenance.is_object());
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(coverage_payload.consistency, name_payload.consistency);
    assert_eq!(coverage_payload.last_updated, name_payload.last_updated);
    assert_eq!(coverage_payload.verified_state, None);
    assert_eq!(
        coverage_payload.declared_state,
        json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_basenames_exact_name_explains_reuse_projection_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9220);
    let token_lineage_id = Uuid::from_u128(0x9221);
    let surface_binding_id = Uuid::from_u128(0x9222);

    database
        .seed_basenames_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;

    let surface_explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/basenames/alice.base.eth/surface-binding")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames surface-binding explain request failed")?;
    let authority_explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/names/basenames/alice.base.eth/authority-control")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames authority-control explain request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/basenames/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames exact-name request failed")?;

    assert_eq!(surface_explain_response.status(), StatusCode::OK);
    assert_eq!(authority_explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let surface_payload: NameResponse = read_json(surface_explain_response).await?;
    let authority_payload: NameResponse = read_json(authority_explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;
    let history = name_payload
        .declared_state
        .get("history")
        .cloned()
        .expect("history summary must be present");

    assert_eq!(surface_payload.data, name_payload.data);
    assert_eq!(surface_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(surface_payload.provenance.is_object());
    assert_eq!(
        surface_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(surface_payload.consistency, name_payload.consistency);
    assert_eq!(surface_payload.last_updated, name_payload.last_updated);
    assert_eq!(surface_payload.verified_state, None);
    assert_eq!(
        surface_payload.declared_state,
        json!({
            "surface_binding": {
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "history": history.clone(),
        })
    );

    assert_eq!(authority_payload.data, name_payload.data);
    assert_eq!(authority_payload.coverage, name_payload.coverage);
    assert!(name_payload.provenance.is_null());
    assert!(authority_payload.provenance.is_object());
    assert_eq!(
        authority_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(authority_payload.consistency, name_payload.consistency);
    assert_eq!(authority_payload.last_updated, name_payload.last_updated);
    assert_eq!(authority_payload.verified_state, None);
    assert_eq!(
        authority_payload.declared_state,
        json!({
            "authority": {
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "control": basenames_exact_name_control_summary(),
        })
    );

    database.cleanup().await?;
    Ok(())
}

async fn seed_ensv2_exact_name_profile_registrar_event(
    database: &TestDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    source_manifest_id: i64,
) -> Result<()> {
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[NormalizedEvent {
            event_identity: format!("api-test:{logical_name_id}:ensv2-registrar-renew"),
            namespace: "ens".to_owned(),
            logical_name_id: Some(logical_name_id.to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RegistrationRenewed".to_owned(),
            source_family: "ens_v2_registrar_l1".to_owned(),
            manifest_version: 11,
            source_manifest_id: Some(source_manifest_id),
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(204),
            block_hash: Some("0xensv2-grant".to_owned()),
            transaction_hash: Some(format!("0xtx:{logical_name_id}:ensv2-registrar-renew")),
            log_index: Some(1),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "event_identity": format!("api-test:{logical_name_id}:ensv2-registrar-renew"),
            }),
            derivation_kind: "ens_v2_registrar".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "duration": 31_536_000_i64,
                "expiry": 1_931_536_000_i64,
            }),
        }],
    )
    .await
    .context("failed to upsert ENSv2 registrar exact-name profile event for API test")?;

    Ok(())
}

async fn seed_ensv2_exact_name_profile_manifests(database: &TestDatabase) -> Result<(i64, i64)> {
    let registry_manifest_id = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-sepolia",
            "ens_v2_sepolia_dev",
            11,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    let registrar_manifest_id = database
        .insert_manifest(
            "ens",
            "ens_v2_registrar_l1",
            "ethereum-sepolia",
            "ens_v2_sepolia_dev",
            11,
            "active",
            "ensip15@ens-normalize-0.1.1",
        )
        .await?;
    database
        .insert_capability_flag(
            registrar_manifest_id,
            "exact_name_profile",
            "supported",
            None,
        )
        .await?;

    Ok((registry_manifest_id, registrar_manifest_id))
}

async fn assign_ensv2_exact_name_profile_source_manifests(
    database: &TestDatabase,
    logical_name_id: &str,
    registry_manifest_id: i64,
    registrar_manifest_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET source_manifest_id = CASE source_family
            WHEN 'ens_v2_registry_l1' THEN $2
            WHEN 'ens_v2_registrar_l1' THEN $3
            ELSE source_manifest_id
        END
        WHERE logical_name_id = $1
          AND source_family IN ('ens_v2_registry_l1', 'ens_v2_registrar_l1')
        "#,
    )
    .bind(logical_name_id)
    .bind(registry_manifest_id)
    .bind(registrar_manifest_id)
    .execute(&database.pool)
    .await
    .context("failed to attach ENSv2 exact-name source manifests for API test")?;

    Ok(())
}
