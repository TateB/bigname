#[tokio::test]
async fn exact_name_contract_returns_frozen_control_resolver_and_history_summaries() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_exact_name_rebuild_inputs(
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
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("exact name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");

    assert_eq!(
        declared_state.get("control"),
        Some(&exact_name_control_summary())
    );
    assert_eq!(
        declared_state.get("resolver"),
        Some(&exact_name_resolver_summary())
    );
    assert_exact_name_history_summary_matches_history_route(
        &database,
        "ens",
        "alice.eth",
        declared_state
            .get("history")
            .expect("history summary must be present"),
    )
    .await?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn coverage_contract_returns_declared_state_explain_with_shared_top_level_coverage()
-> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_exact_name_rebuild_inputs(
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
    let name_declared_state = name_payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert_eq!(coverage_payload.verified_state, None);
    assert_eq!(
        name_declared_state.get("control"),
        Some(&exact_name_control_summary())
    );
    assert_eq!(
        name_declared_state.get("resolver"),
        Some(&exact_name_resolver_summary())
    );
    assert_exact_name_history_summary_matches_history_route(
        &database,
        "ens",
        "alice.eth",
        name_declared_state
            .get("history")
            .expect("history summary must be present"),
    )
    .await?;
    assert_eq!(coverage_payload.declared_state, coverage_payload.coverage);
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
async fn ensv2_sepolia_dev_exact_name_contract_returns_supported_profile_boundary() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:sepolia-dev-profile.eth";
    let resource_id = Uuid::from_u128(0x4400);
    let token_lineage_id = Uuid::from_u128(0x5500);
    let surface_binding_id = Uuid::from_u128(0x6600);

    seed_ens_v2_address_name_rebuild_inputs(
        &database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        "0x0000000000000000000000000000000000000b0b",
        "0x0000000000000000000000000000000000000c0c",
    )
    .await?;
    database
        .insert_name_current_row(ensv2_sepolia_dev_exact_name_row(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/sepolia-dev-profile.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 sepolia-dev exact-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;

    assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(payload.data["namespace"], json!("ens"));
    assert_eq!(
        payload.data["binding_kind"],
        json!("linked_subregistry_path")
    );
    assert_eq!(
        payload.declared_state.get("authority"),
        Some(&ensv2_sepolia_dev_authority_summary(
            resource_id,
            token_lineage_id
        ))
    );
    assert_eq!(
        payload.declared_state.get("control"),
        Some(&ensv2_sepolia_dev_control_summary())
    );
    assert_eq!(
        payload.declared_state.get("resolver"),
        Some(&ensv2_sepolia_dev_resolver_summary())
    );
    assert_eq!(payload.coverage, ensv2_sepolia_dev_exact_name_coverage());
    assert_eq!(
        payload
            .chain_positions
            .as_object()
            .expect("chain_positions must be an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["ethereum-sepolia".to_owned()]
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ensv2_sepolia_dev_coverage_contract_matches_supported_exact_name_boundary() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:sepolia-dev-profile.eth";
    let resource_id = Uuid::from_u128(0x4410);
    let token_lineage_id = Uuid::from_u128(0x5510);
    let surface_binding_id = Uuid::from_u128(0x6610);

    seed_ens_v2_address_name_rebuild_inputs(
        &database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        "0x0000000000000000000000000000000000000b0b",
        "0x0000000000000000000000000000000000000c0c",
    )
    .await?;
    database
        .insert_name_current_row(ensv2_sepolia_dev_exact_name_row(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        ))
        .await?;

    let coverage_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/coverage/ens/sepolia-dev-profile.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 sepolia-dev coverage request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/sepolia-dev-profile.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv2 sepolia-dev exact-name request failed")?;

    assert_eq!(coverage_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let coverage_payload: NameResponse = read_json(coverage_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;
    let expected_coverage = ensv2_sepolia_dev_exact_name_coverage();

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_exact_name_default_provenance(&name_payload);
    assert_diagnostic_name_current_provenance(
        &coverage_payload,
        &[("ens_v2_registry_l1", 11), ("ens_v2_registrar_l1", 11)],
    );
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(coverage_payload.consistency, name_payload.consistency);
    assert_eq!(coverage_payload.last_updated, name_payload.last_updated);
    assert_eq!(coverage_payload.verified_state, None);
    assert_eq!(coverage_payload.coverage, expected_coverage);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert_eq!(coverage_payload.declared_state, expected_coverage);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn surface_binding_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summary()
-> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;

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
        .context("exact name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;
    let name_declared_state = name_payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");
    let history = name_declared_state
        .get("history")
        .cloned()
        .expect("history summary must be present");

    assert_eq!(explain_payload.data, name_payload.data);
    assert_exact_name_default_provenance(&name_payload);
    assert_diagnostic_name_current_provenance(&explain_payload, &[("ens_v1_registrar_l1", 7)]);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("history"),
        Some(&history)
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "surface_binding": exact_name_surface_binding_summary(surface_binding_id),
            "history": history,
        })
    );
    assert_exact_name_history_summary_matches_history_route(
        &database,
        "ens",
        "alice.eth",
        explain_payload
            .declared_state
            .get("history")
            .expect("history summary must be present"),
    )
    .await?;

    database.cleanup().await?;
    Ok(())
}

fn ensv2_sepolia_dev_exact_name_row(
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    let (_, normalized_name) = logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace");

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(SurfaceBindingKind::LinkedSubregistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "ens_v2_registry",
                "authority_key": format!(
                    "ens-v2-registry:ethereum-sepolia:{normalized_name}:0xeac"
                ),
                "registrant": "0x0000000000000000000000000000000000000b0b",
                "expiry": 1_931_536_000_i64,
                "latest_event_kind": "RegistrationRenewed",
            },
            "control": ensv2_sepolia_dev_control_summary(),
            "resolver": ensv2_sepolia_dev_resolver_summary(),
            "history": {
                "surface_head": null,
                "resource_head": null,
            },
        }),
        provenance: json!({
            "normalized_event_ids": [701, 703],
            "raw_fact_refs": [
                {
                    "kind": "raw_log",
                    "chain_id": "ethereum-sepolia",
                    "block_number": 701,
                },
                {
                    "kind": "raw_log",
                    "chain_id": "ethereum-sepolia",
                    "block_number": 703,
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 11,
                    "source_family": "ens_v2_registry_l1",
                    "chain": "ethereum-sepolia",
                    "deployment_profile": "sepolia-dev",
                    "source_manifest_id": null,
                },
                {
                    "manifest_version": 11,
                    "source_family": "ens_v2_registrar_l1",
                    "chain": "ethereum-sepolia",
                    "deployment_profile": "sepolia-dev",
                    "source_manifest_id": null,
                }
            ],
            "execution_trace_id": null,
            "derivation_kind": "name_current_rebuild",
        }),
        coverage: ensv2_sepolia_dev_exact_name_coverage(),
        chain_positions: json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 703,
                "block_hash": "0xensv2-profile-renew",
                "timestamp": "2024-05-31T16:11:43Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-sepolia": "finalized",
            }
        }),
        manifest_version: 11,
        last_recomputed_at: timestamp(1_717_172_703),
    }
}

fn ensv2_sepolia_dev_authority_summary(resource_id: Uuid, token_lineage_id: Uuid) -> Value {
    json!({
        "resource_id": resource_id.to_string(),
        "token_lineage_id": token_lineage_id.to_string(),
        "binding_kind": "linked_subregistry_path",
    })
}

fn ensv2_sepolia_dev_control_summary() -> Value {
    json!({
        "registrant": "0x0000000000000000000000000000000000000b0b",
        "registry_owner": "0x0000000000000000000000000000000000000c0c",
        "latest_event_kind": "AuthorityTransferred",
    })
}

fn ensv2_sepolia_dev_resolver_summary() -> Value {
    json!({
        "chain_id": "ethereum-sepolia",
        "address": "0x0000000000000000000000000000000000000def",
        "latest_event_kind": "ResolverChanged",
    })
}

fn ensv2_sepolia_dev_exact_name_coverage() -> Value {
    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["ens_v2_registry_l1", "ens_v2_registrar_l1"],
        "enumeration_basis": "exact_name_profile",
        "unsupported_reason": null,
    })
}

#[tokio::test]
async fn authority_control_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summaries()
-> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_exact_name_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;

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
        .context("exact name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;
    let name_declared_state = name_payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");
    let authority = exact_name_authority_summary(resource_id, token_lineage_id);
    let control = exact_name_control_summary();

    assert_eq!(explain_payload.data, name_payload.data);
    assert_exact_name_default_provenance(&name_payload);
    assert_diagnostic_name_current_provenance(&explain_payload, &[("ens_v1_registrar_l1", 7)]);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(name_declared_state.get("authority"), Some(&authority));
    assert_eq!(name_declared_state.get("control"), Some(&control));
    assert_eq!(
        explain_payload.declared_state.get("authority"),
        Some(&authority)
    );
    assert_eq!(
        explain_payload.declared_state.get("control"),
        Some(&control)
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "authority": authority,
            "control": control,
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_exact_name_contract_reads_rebuilt_base_authority_projection() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9230);
    let token_lineage_id = Uuid::from_u128(0x9231);
    let surface_binding_id = Uuid::from_u128(0x9232);

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
    assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(payload.data["namespace"], json!("basenames"));
    assert_eq!(
        payload.declared_state.get("control"),
        Some(&basenames_exact_name_control_summary())
    );
    assert_eq!(
        payload.declared_state.get("resolver"),
        Some(&basenames_exact_name_resolver_summary())
    );
    assert_eq!(
        payload
            .declared_state
            .get("history")
            .and_then(|value| value.get("surface_head"))
            .and_then(|value| value.get("event_kind")),
        Some(&json!("ResolverChanged"))
    );
    assert_eq!(
        payload
            .declared_state
            .get("history")
            .and_then(|value| value.get("resource_head"))
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
async fn basenames_exact_name_contract_reads_rebuilt_control_vectors() -> Result<()> {
    let database = HarnessDatabase::new().await?;
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
        seed_basenames_control_vector_rebuild_inputs(
            &database,
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
async fn basenames_coverage_contract_returns_shared_exact_name_coverage() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9240);
    let token_lineage_id = Uuid::from_u128(0x9241);
    let surface_binding_id = Uuid::from_u128(0x9242);

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
    assert_exact_name_default_provenance(&name_payload);
    assert_diagnostic_name_current_provenance(
        &coverage_payload,
        &[
            ("basenames_base_registrar", 3),
            ("basenames_base_registry", 3),
            ("basenames_base_resolver", 4),
        ],
    );
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
async fn basenames_exact_name_explain_contract_reuses_rebuilt_projection_envelope() -> Result<()> {
    let database = HarnessDatabase::new().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9250);
    let token_lineage_id = Uuid::from_u128(0x9251);
    let surface_binding_id = Uuid::from_u128(0x9252);

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
    assert_exact_name_default_provenance(&name_payload);
    assert_diagnostic_name_current_provenance(
        &surface_payload,
        &[
            ("basenames_base_registrar", 3),
            ("basenames_base_registry", 3),
            ("basenames_base_resolver", 4),
        ],
    );
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
            "surface_binding": exact_name_surface_binding_summary(surface_binding_id),
            "history": history.clone(),
        })
    );

    assert_eq!(authority_payload.data, name_payload.data);
    assert_eq!(authority_payload.coverage, name_payload.coverage);
    assert_diagnostic_name_current_provenance(
        &authority_payload,
        &[
            ("basenames_base_registrar", 3),
            ("basenames_base_registry", 3),
            ("basenames_base_resolver", 4),
        ],
    );
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
            "authority": exact_name_authority_summary(resource_id, token_lineage_id),
            "control": basenames_exact_name_control_summary(),
        })
    );

    database.cleanup().await?;
    Ok(())
}

fn assert_exact_name_default_provenance(payload: &NameResponse) {
    assert!(
        payload.provenance.is_null(),
        "exact-name route must omit route-level provenance by default"
    );
}

fn assert_diagnostic_name_current_provenance(
    payload: &NameResponse,
    expected_manifest_versions: &[(&str, i64)],
) {
    let provenance = payload
        .provenance
        .as_object()
        .expect("diagnostic route provenance must be an object");
    assert_eq!(
        provenance.get("derivation_kind").and_then(Value::as_str),
        Some("name_current_rebuild")
    );
    assert!(
        !provenance.contains_key("execution_trace_id"),
        "declared-only diagnostic route provenance must omit execution_trace_id"
    );

    let manifest_versions = provenance
        .get("manifest_versions")
        .and_then(Value::as_array)
        .expect("diagnostic route provenance manifest_versions must be an array");
    assert!(
        !manifest_versions.is_empty(),
        "diagnostic route provenance must include manifest_versions"
    );

    for &(source_family, manifest_version) in expected_manifest_versions {
        assert!(
            manifest_versions.iter().any(|manifest| {
                manifest.get("source_family").and_then(Value::as_str) == Some(source_family)
                    && manifest.get("manifest_version").and_then(Value::as_i64)
                        == Some(manifest_version)
            }),
            "diagnostic route provenance manifest_versions must include {source_family}@{manifest_version}: {manifest_versions:?}"
        );
    }
}
