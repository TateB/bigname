        #[tokio::test]
        async fn smoke_supported_reads_contract_bootstrap() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let ens_l1 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l1",
                    "ethereum-mainnet",
                    "ens_v2",
                    1,
                    "active",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;
            database
                .insert_capability_flag(ens_l1, "declared_children", "supported", None)
                .await?;
            database
                .insert_manifest(
                    "ens",
                    "ens_shadow_registry",
                    "ethereum-mainnet",
                    "ens_shadow",
                    2,
                    "shadow",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/namespaces/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace metadata smoke request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceMetadataResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.active_manifest_count, 1);
            assert_eq!(
                payload.declared_state.active_source_families,
                vec!["ens_v2_registry_l1".to_owned()]
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.consistency, "head");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn namespace_manifests_contract_lists_active_manifests() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let ens_l1 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l1",
                    "ethereum-mainnet",
                    "ens_v2",
                    1,
                    "active",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;
            database
                .insert_capability_flag(ens_l1, "declared_children", "supported", None)
                .await?;
            database
                .insert_capability_flag(
                    ens_l1,
                    "verified_resolution",
                    "shadow",
                    Some("tracked but not yet served"),
                )
                .await?;

            let ens_l2 = database
                .insert_manifest(
                    "ens",
                    "ens_v2_registry_l2",
                    "base-mainnet",
                    "ens_v2_base",
                    2,
                    "active",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;
            database
                .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
                .await?;

            database
                .insert_manifest(
                    "ens",
                    "ens_shadow_registry",
                    "ethereum-mainnet",
                    "ens_shadow",
                    3,
                    "shadow",
                    "ensip15@ens-normalize-0.1.1",
                )
                .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/manifests/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace manifests request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceManifestsResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.manifests.len(), 2);
            assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
            assert_eq!(
                payload.declared_state.manifests[0].source_family,
                "ens_v2_registry_l1"
            );
            assert_eq!(
                payload.declared_state.manifests[0]
                    .capability_flags
                    .get("verified_resolution")
                    .and_then(|flag| flag.notes.as_deref()),
                Some("tracked but not yet served")
            );
            assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
            assert_eq!(
                payload.coverage.enumeration_basis,
                "active manifests for the requested namespace"
            );
            assert_eq!(payload.provenance.manifest_versions.len(), 2);
            assert!(payload.verified_state.is_none());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_returns_declared_rows_sorted_with_declared_only_coverage()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "ens:parent.eth";

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 10),
                    collection_name_surface(
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        11,
                    ),
                    collection_name_surface(
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        12,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for children conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        201,
                        11,
                    ),
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        202,
                        12,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?view=full")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["declared".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "declared_direct_children"
            );
            assert_eq!(payload.coverage.unsupported_reason, None);
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.last_updated, "2024-05-31T16:13:32Z");
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": ["202", "201"],
                    "raw_fact_refs": [
                        {"kind": "raw_log", "block_number": 12},
                        {"kind": "raw_log", "block_number": 11}
                    ],
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registry_l1",
                        "source_manifest_id": null
                    }],
                    "derivation_kind": "children_current_rebuild"
                })
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 12,
                        "block_hash": "0xblock0c",
                        "timestamp": "2026-04-17T00:00:12Z"
                    }
                })
            );

            let child_ids = payload
                .data
                .iter()
                .map(|row| {
                    row.get("logical_name_id")
                        .and_then(Value::as_str)
                        .expect("child row must include logical_name_id")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                child_ids,
                vec!["ens:alice.parent.eth", "ens:bob.parent.eth"]
            );
            assert_eq!(
                payload.data[0].get("surface_class").and_then(Value::as_str),
                Some("declared")
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?view=full&page_size=1")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: ChildrenResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("children first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/parent.eth/children?view=full&page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: ChildrenResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/parent.eth/children?view=full&page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: ChildrenResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "display_name_asc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_returns_ensv2_declared_direct_child_readback() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture = EnsV2DeclaredChildFixture::new(
                "ens:parent.eth",
                "ens:alice.parent.eth",
                Uuid::from_u128(0x9100),
                Uuid::from_u128(0x9101),
                90,
            );
            fixture.seed(&database).await?;

            rebuild_children_current(&database, Some("ens:parent.eth")).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?view=full")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 children request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["declared".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "declared_direct_children"
            );
            assert_eq!(payload.coverage.unsupported_reason, None);
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(
                payload.provenance,
                fixture.expected_children_provenance(&database).await?
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 92,
                        "block_hash": "0xensv2block5c",
                        "timestamp": "2024-05-31T21:14:52Z"
                    }
                })
            );
            assert_eq!(payload.data.len(), 1);
            assert_eq!(
                payload.data[0].get("logical_name_id"),
                Some(&json!("ens:alice.parent.eth"))
            );
            assert_eq!(
                payload.data[0].get("surface_class"),
                Some(&json!("declared"))
            );
            assert_eq!(
                payload.data[0].get("normalized_name"),
                Some(&json!("alice.parent.eth"))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_include_counts_returns_declared_subname_count() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "ens:parent.eth";

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(parent_logical_name_id, "parent.eth", "node:parent.eth", 20),
                    collection_name_surface(
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        21,
                    ),
                    collection_name_surface(
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for children counts conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:alice.parent.eth",
                        "alice.parent.eth",
                        "node:alice.parent.eth",
                        301,
                        21,
                    ),
                    declared_child_row(
                        parent_logical_name_id,
                        "ens:bob.parent.eth",
                        "bob.parent.eth",
                        "node:bob.parent.eth",
                        302,
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for counts conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?view=full&include=counts")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children counts request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert_eq!(payload.declared_state.get("subname_count"), Some(&json!(2)));
            assert_eq!(payload.data.len(), 2);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_returns_basenames_rows_from_base_authority() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "basenames:base.eth";

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(parent_logical_name_id, "base.eth", "node:base.eth", 40),
                    collection_name_surface(
                        "basenames:bob.base.eth",
                        "bob.base.eth",
                        "node:bob.base.eth",
                        41,
                    ),
                    collection_name_surface(
                        "basenames:alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        42,
                    ),
                ],
            )
            .await
            .context("failed to upsert basenames surfaces for children conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        parent_logical_name_id,
                        "basenames:bob.base.eth",
                        "bob.base.eth",
                        "node:bob.base.eth",
                        401,
                        41,
                    ),
                    declared_child_row(
                        parent_logical_name_id,
                        "basenames:alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        402,
                        42,
                    ),
                ],
            )
            .await
            .context("failed to upsert basenames children_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/basenames/base.eth/children?view=full")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames children request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ChildrenResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["declared".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "declared_direct_children"
            );
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.last_updated, "2024-05-31T16:14:02Z");
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": ["402", "401"],
                    "raw_fact_refs": [
                        {"kind": "raw_log", "block_number": 42},
                        {"kind": "raw_log", "block_number": 41}
                    ],
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "basenames_base_registry",
                        "source_manifest_id": null
                    }],
                    "derivation_kind": "children_current_rebuild"
                })
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "base": {
                        "chain_id": "base-mainnet",
                        "block_number": 42,
                        "block_hash": "0xblock2a",
                        "timestamp": "2026-04-17T00:00:42Z"
                    }
                })
            );

            let child_ids = payload
                .data
                .iter()
                .map(|row| {
                    row.get("logical_name_id")
                        .and_then(Value::as_str)
                        .expect("child row must include logical_name_id")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                child_ids,
                vec!["basenames:alice.base.eth", "basenames:bob.base.eth"]
            );
            assert_eq!(
                payload.data[0].get("surface_class").and_then(Value::as_str),
                Some("declared")
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_children_contract_rejects_non_declared_surface_classes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[collection_name_surface(
                    "ens:parent.eth",
                    "parent.eth",
                    "node:parent.eth",
                    30,
                )],
            )
            .await
            .context("failed to upsert parent surface for surface_classes conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/parent.eth/children?surface_classes=declared,linked")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("children unsupported surface_classes request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "unsupported");
            assert_eq!(
                payload.error.message,
                "surface_classes other than declared are not yet supported"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_returns_surface_first_rows_sorted_with_stable_relation_facets()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bbb";
            let alpha_resource_id = Uuid::from_u128(0x8100);
            let alpha_token_lineage_id = Uuid::from_u128(0x8101);
            let alpha_surface_binding_id = Uuid::from_u128(0x8102);
            let beta_resource_id = Uuid::from_u128(0x8200);
            let beta_token_lineage_id = Uuid::from_u128(0x8201);
            let beta_surface_binding_id = Uuid::from_u128(0x8202);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(alpha_token_lineage_id, "0xalpha", 11),
                    address_name_token_lineage(beta_token_lineage_id, "0xbeta", 12),
                ],
            )
            .await
            .context("failed to upsert token lineages for address-name conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        "0xalpha",
                        11,
                    ),
                    address_name_resource(beta_resource_id, Some(beta_token_lineage_id), "0xbeta", 12),
                ],
            )
            .await
            .context("failed to upsert resources for address-name conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 12),
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 11),
                ],
            )
            .await
            .context("failed to upsert name surfaces for address-name conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        beta_surface_binding_id,
                        "ens:beta.eth",
                        beta_resource_id,
                        "0xbeta",
                        12,
                        1_717_173_012,
                    ),
                    address_name_surface_binding(
                        alpha_surface_binding_id,
                        "ens:alpha.eth",
                        alpha_resource_id,
                        "0xalpha",
                        11,
                        1_717_173_011,
                    ),
                ],
            )
            .await
            .context("failed to upsert surface bindings for address-name conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:beta.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "beta.eth",
                        "beta.eth",
                        "node:beta.eth",
                        beta_surface_binding_id,
                        beta_resource_id,
                        Some(beta_token_lineage_id),
                        12,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::TokenHolder,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        11,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        alpha_resource_id,
                        Some(alpha_token_lineage_id),
                        11,
                    ),
                ],
            )
            .await
            .context("failed to upsert address_names_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: AddressNamesResponse = read_json(response).await?;
            assert!(
                payload
                    .declared_state
                    .as_object()
                    .map(|value| value.is_empty())
                    .unwrap_or(false)
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ensv1_registry_path".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "surface_current_relations"
            );
            assert_eq!(payload.page.sort, "display_name_asc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(payload.consistency, "finalized");

            let logical_name_ids = payload
                .data
                .iter()
                .map(|row| {
                    row.get("logical_name_id")
                        .and_then(Value::as_str)
                        .expect("address-name row must include logical_name_id")
                })
                .collect::<Vec<_>>();
            assert_eq!(logical_name_ids, vec!["ens:alpha.eth", "ens:beta.eth"]);
            assert_eq!(
                payload.data[0].get("relation_facets"),
                Some(&json!(["registrant", "token_holder"]))
            );
            assert_eq!(
                payload.data[1].get("relation_facets"),
                Some(&json!(["effective_controller"]))
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: AddressNamesResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("address names first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: AddressNamesResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address names replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: AddressNamesResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "display_name_asc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_honors_namespace_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let ens_resource_id = Uuid::from_u128(0x8300);
            let ens_token_lineage_id = Uuid::from_u128(0x8301);
            let ens_surface_binding_id = Uuid::from_u128(0x8302);
            let base_resource_id = Uuid::from_u128(0x8400);
            let base_surface_binding_id = Uuid::from_u128(0x8402);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    ens_token_lineage_id,
                    "0xens",
                    21,
                )],
            )
            .await
            .context("failed to upsert filtered token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(ens_resource_id, Some(ens_token_lineage_id), "0xens", 21),
                    address_name_resource(base_resource_id, None, "0xbase", 22),
                ],
            )
            .await
            .context("failed to upsert filtered resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:alice.eth", "alice.eth", "node:alice.eth", 21),
                    collection_name_surface(
                        "basenames:alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        ens_surface_binding_id,
                        "ens:alice.eth",
                        ens_resource_id,
                        "0xens",
                        21,
                        1_717_173_021,
                    ),
                    address_name_surface_binding(
                        base_surface_binding_id,
                        "basenames:alice.base.eth",
                        base_resource_id,
                        "0xbase",
                        22,
                        1_717_173_022,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered surface bindings for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:alice.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alice.eth",
                        "alice.eth",
                        "node:alice.eth",
                        ens_surface_binding_id,
                        ens_resource_id,
                        Some(ens_token_lineage_id),
                        21,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:alice.base.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        base_surface_binding_id,
                        base_resource_id,
                        None,
                        22,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered address_names_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?namespace=ens&relation=registrant"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("filtered address names request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: AddressNamesResponse = read_json(response).await?;
            assert_eq!(payload.data.len(), 1);
            assert_eq!(
                payload.data[0].get("logical_name_id"),
                Some(&Value::String("ens:alice.eth".to_owned()))
            );
            assert_eq!(
                payload.data[0].get("relation_facets"),
                Some(&json!(["registrant"]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_dedupe_by_resource_changes_grouping_only() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000def";
            let shared_resource_id = Uuid::from_u128(0x8500);
            let shared_token_lineage_id = Uuid::from_u128(0x8501);
            let alpha_surface_binding_id = Uuid::from_u128(0x8502);
            let beta_surface_binding_id = Uuid::from_u128(0x8503);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    shared_token_lineage_id,
                    "0xshared",
                    31,
                )],
            )
            .await
            .context("failed to upsert shared token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[address_name_resource(
                    shared_resource_id,
                    Some(shared_token_lineage_id),
                    "0xshared",
                    31,
                )],
            )
            .await
            .context("failed to upsert shared resource for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:beta.eth", "beta.eth", "node:beta.eth", 31),
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 31),
                ],
            )
            .await
            .context("failed to upsert shared name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        beta_surface_binding_id,
                        "ens:beta.eth",
                        shared_resource_id,
                        "0xshared",
                        31,
                        1_717_173_031,
                    ),
                    address_name_surface_binding(
                        alpha_surface_binding_id,
                        "ens:alpha.eth",
                        shared_resource_id,
                        "0xshared",
                        31,
                        1_717_173_031,
                    ),
                ],
            )
            .await
            .context("failed to upsert shared surface bindings for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "ens:beta.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "beta.eth",
                        "beta.eth",
                        "node:beta.eth",
                        beta_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                    address_name_current_row(
                        address,
                        "ens:alpha.eth",
                        bigname_storage::AddressNameRelation::TokenHolder,
                        "alpha.eth",
                        "alpha.eth",
                        "node:alpha.eth",
                        alpha_surface_binding_id,
                        shared_resource_id,
                        Some(shared_token_lineage_id),
                        31,
                    ),
                ],
            )
            .await
            .context("failed to upsert shared address_names_current rows for conformance")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?dedupe_by=surface"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("surface-dedupe address names request failed")?;
            let surface_payload: AddressNamesResponse = read_json(surface_response).await?;
            assert_eq!(surface_payload.data.len(), 2);

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?dedupe_by=resource"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource-dedupe address names request failed")?;

            assert_eq!(resource_response.status(), StatusCode::OK);

            let resource_payload: AddressNamesResponse = read_json(resource_response).await?;
            assert_eq!(resource_payload.data.len(), 1);
            assert_eq!(
                resource_payload.data[0].get("logical_name_id"),
                Some(&Value::String("ens:alpha.eth".to_owned()))
            );
            assert_eq!(
                resource_payload.data[0].get("resource_id"),
                Some(&Value::String(shared_resource_id.to_string()))
            );
            assert_eq!(
                resource_payload.data[0].get("relation_facets"),
                Some(&json!([
                    "registrant",
                    "token_holder",
                    "effective_controller"
                ]))
            );
            assert_eq!(resource_payload.coverage, surface_payload.coverage);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_returns_basenames_base_authority_relation_facets() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bcd";
            let resource_id = Uuid::from_u128(0x85a0);
            let token_lineage_id = Uuid::from_u128(0x85a1);
            let surface_binding_id = Uuid::from_u128(0x85a2);

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    token_lineage_id,
                    "0xbase-alpha",
                    41,
                )],
            )
            .await
            .context("failed to upsert basenames token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[address_name_resource(
                    resource_id,
                    Some(token_lineage_id),
                    "0xbase-alpha",
                    41,
                )],
            )
            .await
            .context("failed to upsert basenames resource for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[collection_name_surface(
                    "basenames:alice.base.eth",
                    "alice.base.eth",
                    "node:alice.base.eth",
                    41,
                )],
            )
            .await
            .context("failed to upsert basenames surface for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    surface_binding_id,
                    "basenames:alice.base.eth",
                    resource_id,
                    "0xbase-alpha",
                    41,
                    1_717_173_041,
                )],
            )
            .await
            .context("failed to upsert basenames surface binding for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[
                    address_name_current_row(
                        address,
                        "basenames:alice.base.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        surface_binding_id,
                        resource_id,
                        Some(token_lineage_id),
                        41,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:alice.base.eth",
                        bigname_storage::AddressNameRelation::TokenHolder,
                        "alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        surface_binding_id,
                        resource_id,
                        Some(token_lineage_id),
                        41,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:alice.base.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "alice.base.eth",
                        "alice.base.eth",
                        "node:alice.base.eth",
                        surface_binding_id,
                        resource_id,
                        Some(token_lineage_id),
                        41,
                    ),
                ],
            )
            .await
            .context("failed to upsert basenames address_names_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?namespace=basenames"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames address names request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: AddressNamesResponse = read_json(response).await?;
            assert_eq!(payload.data.len(), 1);
            assert_eq!(
                payload.data[0].get("logical_name_id"),
                Some(&Value::String("basenames:alice.base.eth".to_owned()))
            );
            assert_eq!(
                payload.data[0].get("relation_facets"),
                Some(&json!([
                    "registrant",
                    "token_holder",
                    "effective_controller"
                ]))
            );
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ensv1_registry_path".to_owned()]
            );
            assert!(payload.data[0].get("role_summary").is_none());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_returns_basenames_base_authority_relation_facets_across_control_vectors()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let cases = [
                (
                    "nft-only.base.eth",
                    BasenamesControlVectorScenario::NftOnly,
                    0x86a0_u128,
                ),
                (
                    "management-only.base.eth",
                    BasenamesControlVectorScenario::ManagementOnly,
                    0x86b0_u128,
                ),
                (
                    "full-transfer.base.eth",
                    BasenamesControlVectorScenario::FullTransfer,
                    0x86c0_u128,
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
            }
            rebuild_address_names_current(&database, None).await?;

            for (name, scenario, _) in cases {
                let logical_name_id = format!("basenames:{name}");
                let holder_response = app_router(database.app_state())
                    .oneshot(
                        Request::builder()
                            .uri(format!(
                                "/v1/addresses/{}/names?namespace=basenames",
                                scenario.current_token_subject()
                            ))
                            .body(Body::empty())
                            .expect("request must build"),
                    )
                    .await
                    .with_context(|| format!("Basenames address names request failed for {name}"))?;

                assert_eq!(holder_response.status(), StatusCode::OK);
                let holder_payload: AddressNamesResponse = read_json(holder_response).await?;
                assert_eq!(holder_payload.data.len(), 1);
                assert_eq!(
                    holder_payload.data[0].get("logical_name_id"),
                    Some(&json!(logical_name_id))
                );
                assert_eq!(
                    holder_payload.data[0].get("relation_facets"),
                    Some(&match scenario {
                        BasenamesControlVectorScenario::FullTransfer =>
                            json!(["registrant", "token_holder", "effective_controller"]),
                        _ => json!(["registrant", "token_holder"]),
                    })
                );
                assert_eq!(
                    holder_payload.coverage.source_classes_considered,
                    vec!["ensv1_registry_path".to_owned()]
                );

                if scenario.current_effective_controller() != scenario.current_token_subject() {
                    let controller_response = app_router(database.app_state())
                        .oneshot(
                            Request::builder()
                                .uri(format!(
                                    "/v1/addresses/{}/names?namespace=basenames",
                                    scenario.current_effective_controller()
                                ))
                                .body(Body::empty())
                                .expect("request must build"),
                        )
                        .await
                        .with_context(|| {
                            format!("Basenames controller address request failed for {name}")
                        })?;

                    assert_eq!(controller_response.status(), StatusCode::OK);
                    let controller_payload: AddressNamesResponse = read_json(controller_response).await?;
                    assert_eq!(controller_payload.data.len(), 1);
                    assert_eq!(
                        controller_payload.data[0].get("logical_name_id"),
                        Some(&json!(logical_name_id))
                    );
                    assert_eq!(
                        controller_payload.data[0].get("relation_facets"),
                        Some(&json!(["effective_controller"]))
                    );
                }

                if let Some(previous_controller) = scenario.previous_effective_controller() {
                    let previous_response = app_router(database.app_state())
                        .oneshot(
                            Request::builder()
                                .uri(format!(
                                    "/v1/addresses/{previous_controller}/names?namespace=basenames"
                                ))
                                .body(Body::empty())
                                .expect("request must build"),
                        )
                        .await
                        .with_context(|| {
                            format!("Basenames previous controller request failed for {name}")
                        })?;

                    assert_eq!(previous_response.status(), StatusCode::OK);
                    let previous_payload: AddressNamesResponse = read_json(previous_response).await?;
                    assert!(previous_payload.data.is_empty());
                }
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_include_role_summary_is_additive_and_preserves_base_collection_behavior()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000fed";
            let resource_id = Uuid::from_u128(0x8600);
            let token_lineage_id = Uuid::from_u128(0x8601);
            let surface_binding_id = Uuid::from_u128(0x8602);
            let subject = "0x0000000000000000000000000000000000000abc";
            let other_subject = "0x0000000000000000000000000000000000000def";

            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(token_lineage_id, "0xalpha", 61)],
            )
            .await
            .context("failed to upsert token lineage for role-summary conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[address_name_resource(
                    resource_id,
                    Some(token_lineage_id),
                    "0xalpha",
                    61,
                )],
            )
            .await
            .context("failed to upsert resource for role-summary conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:alpha.eth", "alpha.eth", "node:alpha.eth", 61),
                    collection_name_surface(
                        "ens:child-one.alpha.eth",
                        "child-one.alpha.eth",
                        "node:child-one.alpha.eth",
                        62,
                    ),
                    collection_name_surface(
                        "ens:child-two.alpha.eth",
                        "child-two.alpha.eth",
                        "node:child-two.alpha.eth",
                        63,
                    ),
                ],
            )
            .await
            .context("failed to upsert surfaces for role-summary conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    surface_binding_id,
                    "ens:alpha.eth",
                    resource_id,
                    "0xalpha",
                    61,
                    1_717_173_061,
                )],
            )
            .await
            .context("failed to upsert surface binding for role-summary conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:alpha.eth",
                    bigname_storage::AddressNameRelation::Registrant,
                    "alpha.eth",
                    "alpha.eth",
                    "node:alpha.eth",
                    surface_binding_id,
                    resource_id,
                    Some(token_lineage_id),
                    61,
                )],
            )
            .await
            .context("failed to upsert address_names_current rows for role-summary conformance")?;
            database
                .insert_name_current_row(address_name_name_current_row(
                    "ens:alpha.eth",
                    "alpha.eth",
                    "alpha.eth",
                    "node:alpha.eth",
                    surface_binding_id,
                    resource_id,
                    Some(token_lineage_id),
                    64,
                    json!({
                        "registration": {
                            "status": "active",
                            "authority_kind": "registrar",
                        },
                        "control": {
                            "status": "wrapped",
                            "expiry": "2026-09-01T00:00:00Z",
                            "registrant": address,
                            "registry_owner": subject,
                            "latest_event_kind": "NameWrapped",
                        },
                        "resolver": {
                            "chain_id": "ethereum-mainnet",
                            "address": "0x0000000000000000000000000000000000000aaa",
                            "latest_event_kind": "ResolverChanged",
                        },
                        "record_inventory": {
                            "status": "supported",
                            "count": 2,
                        },
                        "history": {
                            "surface_head": null,
                            "resource_head": null,
                        },
                    }),
                ))
                .await?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    declared_child_row(
                        "ens:alpha.eth",
                        "ens:child-one.alpha.eth",
                        "child-one.alpha.eth",
                        "node:child-one.alpha.eth",
                        701,
                        62,
                    ),
                    declared_child_row(
                        "ens:alpha.eth",
                        "ens:child-two.alpha.eth",
                        "child-two.alpha.eth",
                        "node:child-two.alpha.eth",
                        702,
                        63,
                    ),
                ],
            )
            .await
            .context("failed to upsert children_current rows for role-summary conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(resource_id, subject, PermissionScope::Resource, 7, 71),
                    permission_current_row(
                        resource_id,
                        subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                        },
                        8,
                        72,
                    ),
                    permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 73),
                ],
            )
            .await
            .context("failed to upsert permissions_current rows for role-summary conformance")?;

            let base_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("base address names request failed")?;
            let include_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?include=role_summary"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("role summary address names request failed")?;

            assert_eq!(base_response.status(), StatusCode::OK);
            assert_eq!(include_response.status(), StatusCode::OK);

            let base_payload: AddressNamesResponse = read_json(base_response).await?;
            let payload: AddressNamesResponse = read_json(include_response).await?;

            assert_eq!(payload.coverage, base_payload.coverage);
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ensv1_registry_path"]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "surface_current_relations"
            );
            assert_eq!(payload.page, base_payload.page);
            assert_eq!(payload.declared_state, base_payload.declared_state);
            assert_eq!(payload.consistency, base_payload.consistency);
            assert_eq!(payload.data.len(), base_payload.data.len());

            let base_row = base_payload.data[0]
                .as_object()
                .expect("base address-name row must be an object");
            let include_row = payload.data[0]
                .as_object()
                .expect("role-summary address-name row must be an object");
            let base_keys = base_row.keys().cloned().collect::<BTreeSet<_>>();
            let include_keys = include_row.keys().cloned().collect::<BTreeSet<_>>();

            assert_eq!(
                include_keys
                    .difference(&base_keys)
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![
                    "expiry".to_owned(),
                    "record_count".to_owned(),
                    "role_summary".to_owned(),
                    "status".to_owned(),
                    "subname_count".to_owned(),
                ]
            );

            for key in &base_keys {
                assert_eq!(
                    include_row.get(key),
                    base_row.get(key),
                    "include=role_summary must preserve base field {key}"
                );
            }

            assert_eq!(payload.data[0].get("status"), Some(&json!("wrapped")));
            assert_eq!(
                payload.data[0].get("expiry"),
                Some(&json!("2026-09-01T00:00:00Z"))
            );
            assert_eq!(payload.data[0].get("record_count"), Some(&json!(2)));
            assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
            assert_eq!(
                payload.data[0].get("role_summary"),
                Some(&json!({
                    "subjects": [
                        {
                            "subject": subject,
                            "scopes": [
                                {
                                    "scope": {
                                        "kind": "resolver",
                                        "detail": {
                                            "chain_id": "ethereum-mainnet",
                                            "resolver_address": "0x0000000000000000000000000000000000000aaa",
                                        },
                                    },
                                    "effective_powers": ["set_resolver", "create_subnames"],
                                },
                                {
                                    "scope": {
                                        "kind": "resource",
                                        "detail": {},
                                    },
                                    "effective_powers": ["set_resolver", "set_records"],
                                },
                            ],
                        },
                        {
                            "subject": other_subject,
                            "scopes": [
                                {
                                    "scope": {
                                        "kind": "registry",
                                        "detail": {},
                                    },
                                    "effective_powers": ["set_resolver", "set_records"],
                                },
                            ],
                        },
                    ],
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_names_contract_include_role_summary_reads_ensv2_projection_outputs()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:bob.alice.eth";
            let normalized_name = "bob.alice.eth";
            let namehash = "namehash:bob.alice.eth";
            let resource_id = Uuid::from_u128(0x8c10);
            let token_lineage_id = Uuid::from_u128(0x8c11);
            let surface_binding_id = Uuid::from_u128(0x8c12);
            let registrant = "0x0000000000000000000000000000000000000b0b";
            let controller = "0x0000000000000000000000000000000000000c0c";
            let resolver_address = "0x0000000000000000000000000000000000000abc";

            seed_ens_v2_address_name_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
                registrant,
                controller,
            )
            .await?;
            rebuild_address_names_current(&database, Some(controller)).await?;

            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let selector_count = record_inventory_row
                .selectors
                .as_array()
                .expect("record_inventory_current selectors must be an array")
                .len();
            database
                .insert_record_inventory_current_row(record_inventory_row)
                .await?;

            let mut name_row = address_name_name_current_row(
                logical_name_id,
                normalized_name,
                normalized_name,
                namehash,
                surface_binding_id,
                resource_id,
                Some(token_lineage_id),
                206,
                json!({
                    "registration": {
                        "status": "active",
                        "authority_kind": "ens_v2_registry",
                    },
                    "control": {
                        "status": "active",
                        "expiry": "2030-03-17T17:46:40Z",
                        "registrant": registrant,
                        "registry_owner": controller,
                        "latest_event_kind": "AuthorityTransferred",
                    },
                    "resolver": {
                        "chain_id": "ethereum-sepolia",
                        "address": resolver_address,
                        "latest_event_kind": "ResolverChanged",
                    },
                    "record_inventory": {
                        "status": "supported",
                        "count": selector_count,
                    },
                    "history": {
                        "surface_head": null,
                        "resource_head": null,
                    },
                }),
            );
            name_row.binding_kind = Some(bigname_storage::SurfaceBindingKind::LinkedSubregistryPath);
            name_row.provenance = json!({
                "normalized_event_ids": [206],
                "raw_fact_refs": [{
                    "kind": "raw_log",
                    "chain_id": "ethereum-sepolia",
                    "block_number": 206,
                }],
                "manifest_versions": [{
                    "manifest_version": 11,
                    "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                    "source_manifest_id": null,
                }],
                "execution_trace_id": null,
                "derivation_kind": "name_current_supporting_projection",
            });
            name_row.coverage = json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [ENSV2_REGISTRY_SOURCE_FAMILY],
                "unsupported_reason": "name_current is seeded here only to support address role-summary expansion",
                "enumeration_basis": "address_name_role_summary_support",
            });
            name_row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-sepolia",
                    "block_number": 206,
                    "block_hash": "0xensv2-regen",
                    "timestamp": "2024-05-31T16:10:06Z",
                }
            });
            name_row.canonicality_summary = json!({
                "status": "finalized",
                "chains": {
                    "ethereum-sepolia": "finalized",
                }
            });
            name_row.manifest_version = 11;
            database.insert_name_current_row(name_row).await?;

            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        "ens:carol.bob.alice.eth",
                        "carol.bob.alice.eth",
                        "namehash:carol.bob.alice.eth",
                        207,
                    ),
                    collection_name_surface(
                        "ens:dave.bob.alice.eth",
                        "dave.bob.alice.eth",
                        "namehash:dave.bob.alice.eth",
                        208,
                    ),
                    collection_name_surface(
                        "ens:eve.carol.bob.alice.eth",
                        "eve.carol.bob.alice.eth",
                        "namehash:eve.carol.bob.alice.eth",
                        209,
                    ),
                ],
            )
            .await
            .context("failed to upsert ENSv2 child surfaces for address role-summary conformance")?;
            bigname_storage::upsert_children_current_rows(
                &database.pool,
                &[
                    ens_v2_declared_child_row(
                        logical_name_id,
                        "ens:carol.bob.alice.eth",
                        "carol.bob.alice.eth",
                        "namehash:carol.bob.alice.eth",
                        801,
                        207,
                    ),
                    ens_v2_declared_child_row(
                        logical_name_id,
                        "ens:dave.bob.alice.eth",
                        "dave.bob.alice.eth",
                        "namehash:dave.bob.alice.eth",
                        802,
                        208,
                    ),
                ],
            )
            .await
            .context("failed to upsert ENSv2 children_current rows for address role-summary conformance")?;

            seed_ens_v2_event_fixture_inputs(
                &database.pool,
                &[
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:bob.alice.eth:resource-permission",
                        logical_name_id,
                        resource_id,
                        controller,
                        PermissionScope::Resource,
                        &["set_resolver", "set_records"],
                        11,
                        209,
                        0,
                    ),
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:bob.alice.eth:resolver-permission",
                        logical_name_id,
                        resource_id,
                        controller,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-sepolia".to_owned(),
                            resolver_address: resolver_address.to_owned(),
                        },
                        &["set_resolver", "create_subnames"],
                        12,
                        210,
                        0,
                    ),
                ],
            )
            .await?;
            rebuild_permissions_current(&database, Some(resource_id)).await?;

            let base_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{controller}/names?namespace=ens"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 base address names request failed")?;
            let include_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{controller}/names?namespace=ens&include=role_summary"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 role_summary address names request failed")?;

            assert_eq!(base_response.status(), StatusCode::OK);
            assert_eq!(include_response.status(), StatusCode::OK);

            let base_payload: AddressNamesResponse = read_json(base_response).await?;
            let payload: AddressNamesResponse = read_json(include_response).await?;

            assert_eq!(base_payload.data.len(), 1);
            assert_eq!(payload.data.len(), 1);
            assert_eq!(
                base_payload.data[0].get("logical_name_id"),
                Some(&json!(logical_name_id))
            );
            assert_eq!(
                base_payload.data[0].get("binding_kind"),
                Some(&json!("linked_subregistry_path"))
            );
            assert_eq!(
                base_payload.data[0].get("relation_facets"),
                Some(&json!(["effective_controller"]))
            );
            assert!(base_payload.data[0].get("role_summary").is_none());
            assert!(base_payload.data[0].get("subname_count").is_none());
            assert!(base_payload.data[0].get("record_count").is_none());
            assert_eq!(payload.coverage, base_payload.coverage);
            assert_eq!(payload.page, base_payload.page);
            assert_eq!(payload.declared_state, base_payload.declared_state);
            assert_eq!(payload.consistency, base_payload.consistency);

            let base_row = base_payload.data[0]
                .as_object()
                .expect("base ENSv2 address-name row must be an object");
            let include_row = payload.data[0]
                .as_object()
                .expect("ENSv2 role-summary address-name row must be an object");
            let base_keys = base_row.keys().cloned().collect::<BTreeSet<_>>();
            let include_keys = include_row.keys().cloned().collect::<BTreeSet<_>>();
            assert_eq!(
                include_keys
                    .difference(&base_keys)
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![
                    "expiry".to_owned(),
                    "record_count".to_owned(),
                    "role_summary".to_owned(),
                    "status".to_owned(),
                    "subname_count".to_owned(),
                ]
            );
            for key in &base_keys {
                assert_eq!(
                    include_row.get(key),
                    base_row.get(key),
                    "include=role_summary must preserve ENSv2 base field {key}"
                );
            }

            assert_eq!(payload.data[0].get("status"), Some(&json!("active")));
            assert_eq!(
                payload.data[0].get("expiry"),
                Some(&json!("2030-03-17T17:46:40Z"))
            );
            assert_eq!(payload.data[0].get("subname_count"), Some(&json!(2)));
            assert_eq!(
                payload.data[0].get("record_count"),
                Some(&json!(selector_count))
            );
            assert_eq!(
                payload.data[0].get("role_summary"),
                Some(&json!({
                    "subjects": [{
                        "subject": controller,
                        "scopes": [
                            {
                                "scope": {
                                    "kind": "resolver",
                                    "detail": {
                                        "chain_id": "ethereum-sepolia",
                                        "resolver_address": resolver_address,
                                    },
                                },
                                "effective_powers": ["set_resolver", "create_subnames"],
                            },
                            {
                                "scope": {
                                    "kind": "resource",
                                    "detail": {},
                                },
                                "effective_powers": ["set_resolver", "set_records"],
                            },
                        ],
                    }],
                }))
            );
            assert!(
                payload.provenance.get("execution_trace_id").is_none(),
                "declared-only address-name provenance must omit execution_trace_id"
            );
            assert!(
                payload.provenance["manifest_versions"]
                    .as_array()
                    .expect("address-name provenance manifest_versions must be an array")
                    .iter()
                    .any(|manifest| manifest.get("source_family")
                        == Some(&json!(ENSV2_REGISTRY_SOURCE_FAMILY)))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn collection_read_performance_conformance_baseline_collections() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let parent_logical_name_id = "ens:perf-parent.eth";
            let address = "0x0000000000000000000000000000000000000c0f";
            let resource_id = Uuid::from_u128(0xc400);

            let child_count = 32;
            let mut surfaces = vec![collection_name_surface(
                parent_logical_name_id,
                "perf-parent.eth",
                "node:perf-parent.eth",
                80,
            )];
            let mut children = Vec::new();
            for index in (0..child_count).rev() {
                let display_name = format!("perf-child-{index:02}.perf-parent.eth");
                let logical_name_id = format!("ens:{display_name}");
                let block_number = 81 + index as i64;
                surfaces.push(collection_name_surface(
                    &logical_name_id,
                    &display_name,
                    &format!("node:{display_name}"),
                    block_number,
                ));
                children.push(declared_child_row(
                    parent_logical_name_id,
                    &logical_name_id,
                    &display_name,
                    &format!("node:{display_name}"),
                    900 + index as i64,
                    block_number,
                ));
            }

            bigname_storage::upsert_name_surfaces(&database.pool, &surfaces)
                .await
                .context("failed to upsert larger children surfaces for conformance")?;
            bigname_storage::upsert_children_current_rows(&database.pool, &children)
                .await
                .context("failed to upsert larger children rows for conformance")?;

            let address_name_count = 27;
            let mut token_lineages = Vec::new();
            let mut resources = Vec::new();
            let mut address_surfaces = Vec::new();
            let mut surface_bindings = Vec::new();
            let mut address_rows = Vec::new();
            for index in (0..address_name_count).rev() {
                let resource_id = Uuid::from_u128(0xc000 + index as u128);
                let token_lineage_id = Uuid::from_u128(0xc100 + index as u128);
                let surface_binding_id = Uuid::from_u128(0xc200 + index as u128);
                let display_name = format!("perf-address-{index:02}.eth");
                let logical_name_id = format!("ens:{display_name}");
                let block_hash = format!("0xperfaddress{index:02x}");
                let block_number = 120 + index as i64;

                token_lineages.push(address_name_token_lineage(
                    token_lineage_id,
                    &block_hash,
                    block_number,
                ));
                resources.push(address_name_resource(
                    resource_id,
                    Some(token_lineage_id),
                    &block_hash,
                    block_number,
                ));
                address_surfaces.push(collection_name_surface(
                    &logical_name_id,
                    &display_name,
                    &format!("node:{display_name}"),
                    block_number,
                ));
                surface_bindings.push(address_name_surface_binding(
                    surface_binding_id,
                    &logical_name_id,
                    resource_id,
                    &block_hash,
                    block_number,
                    1_717_173_120 + index as i64,
                ));
                address_rows.push(address_name_current_row(
                    address,
                    &logical_name_id,
                    bigname_storage::AddressNameRelation::Registrant,
                    &display_name,
                    &display_name,
                    &format!("node:{display_name}"),
                    surface_binding_id,
                    resource_id,
                    Some(token_lineage_id),
                    block_number,
                ));
            }
            resources.push(address_name_resource(
                resource_id,
                None,
                "0xperfpermissions",
                149,
            ));

            bigname_storage::upsert_token_lineages(&database.pool, &token_lineages)
                .await
                .context("failed to upsert larger address-name token lineages")?;
            bigname_storage::upsert_resources(&database.pool, &resources)
                .await
                .context("failed to upsert larger address-name resources")?;
            bigname_storage::upsert_name_surfaces(&database.pool, &address_surfaces)
                .await
                .context("failed to upsert larger address-name surfaces")?;
            bigname_storage::upsert_surface_bindings(&database.pool, &surface_bindings)
                .await
                .context("failed to upsert larger address-name surface bindings")?;
            bigname_storage::upsert_address_names_current_rows(&database.pool, &address_rows)
                .await
                .context("failed to upsert larger address-name rows")?;

            let permission_count = 18;
            let permission_rows = (0..permission_count)
                .rev()
                .map(|index| {
                    permission_current_row(
                        resource_id,
                        &format!("0x000000000000000000000000000000000000{index:04x}"),
                        PermissionScope::Resource,
                        30 + index as i64,
                        150 + index as i64,
                    )
                })
                .collect::<Vec<_>>();
            bigname_storage::upsert_permissions_current_rows(&database.pool, &permission_rows)
                .await
                .context("failed to upsert larger permissions rows")?;

            let children_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/perf-parent.eth/children?view=full")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger children base request failed")?;
            assert_eq!(children_response.status(), StatusCode::OK);
            let children_payload: ChildrenResponse = read_json(children_response).await?;
            assert_eq!(children_payload.data.len(), child_count);
            assert_eq!(children_payload.page.sort, "display_name_asc");

            let children_first_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/perf-parent.eth/children?view=full&page_size=11")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger children first page request failed")?;
            let children_first_payload: ChildrenResponse =
                read_json(children_first_response).await?;
            let children_cursor = children_first_payload
                .page
                .next_cursor
                .clone()
                .expect("larger children first page must include next_cursor");
            let children_second_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/perf-parent.eth/children?view=full&page_size=11&cursor={children_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger children second page request failed")?;
            let children_second_payload: ChildrenResponse =
                read_json(children_second_response).await?;
            let children_third_cursor = children_second_payload
                .page
                .next_cursor
                .clone()
                .expect("larger children second page must include next_cursor");
            let children_third_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/perf-parent.eth/children?view=full&page_size=11&cursor={children_third_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger children third page request failed")?;
            let children_third_payload: ChildrenResponse =
                read_json(children_third_response).await?;
            let children_replay_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/names/ens/perf-parent.eth/children?view=full&page_size=11&cursor={children_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger children replay page request failed")?;
            let children_replay_payload: ChildrenResponse =
                read_json(children_replay_response).await?;
            assert_replay_stable_pagination(
                &children_payload.data,
                &children_payload.page,
                &children_first_payload.data,
                &children_first_payload.page,
                &children_second_payload.data,
                &children_second_payload.page,
                &children_replay_payload.data,
                &children_replay_payload.page,
                "display_name_asc",
                50,
                11,
            );
            assert_page_walk_matches_base_prefix(
                &children_payload.data,
                &[
                    &children_first_payload.data,
                    &children_second_payload.data,
                    &children_third_payload.data,
                ],
            );
            assert_eq!(children_third_payload.page.next_cursor, None);

            let address_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger address names base request failed")?;
            assert_eq!(address_response.status(), StatusCode::OK);
            let address_payload: AddressNamesResponse = read_json(address_response).await?;
            assert_eq!(address_payload.data.len(), address_name_count);
            assert_eq!(address_payload.page.sort, "display_name_asc");

            let address_first_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/addresses/{address}/names?page_size=9"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger address names first page request failed")?;
            let address_first_payload: AddressNamesResponse =
                read_json(address_first_response).await?;
            let address_cursor = address_first_payload
                .page
                .next_cursor
                .clone()
                .expect("larger address names first page must include next_cursor");
            let address_second_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=9&cursor={address_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger address names second page request failed")?;
            let address_second_payload: AddressNamesResponse =
                read_json(address_second_response).await?;
            let address_third_cursor = address_second_payload
                .page
                .next_cursor
                .clone()
                .expect("larger address names second page must include next_cursor");
            let address_third_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=9&cursor={address_third_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger address names third page request failed")?;
            let address_third_payload: AddressNamesResponse =
                read_json(address_third_response).await?;
            let address_replay_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/addresses/{address}/names?page_size=9&cursor={address_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger address names replay page request failed")?;
            let address_replay_payload: AddressNamesResponse =
                read_json(address_replay_response).await?;
            assert_replay_stable_pagination(
                &address_payload.data,
                &address_payload.page,
                &address_first_payload.data,
                &address_first_payload.page,
                &address_second_payload.data,
                &address_second_payload.page,
                &address_replay_payload.data,
                &address_replay_payload.page,
                "display_name_asc",
                50,
                9,
            );
            assert_page_walk_matches_base_prefix(
                &address_payload.data,
                &[
                    &address_first_payload.data,
                    &address_second_payload.data,
                    &address_third_payload.data,
                ],
            );
            assert_eq!(address_third_payload.page.next_cursor, None);

            let permissions_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger permissions base request failed")?;
            assert_eq!(permissions_response.status(), StatusCode::OK);
            let permissions_payload: ResourcePermissionsResponse =
                read_json(permissions_response).await?;
            assert_eq!(permissions_payload.data.len(), permission_count);
            assert_eq!(permissions_payload.page.sort, "subject_scope_asc");

            let permissions_first_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions?page_size=7"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger permissions first page request failed")?;
            let permissions_first_payload: ResourcePermissionsResponse =
                read_json(permissions_first_response).await?;
            let permissions_cursor = permissions_first_payload
                .page
                .next_cursor
                .clone()
                .expect("larger permissions first page must include next_cursor");
            let permissions_second_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=7&cursor={permissions_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger permissions second page request failed")?;
            let permissions_second_payload: ResourcePermissionsResponse =
                read_json(permissions_second_response).await?;
            let permissions_third_cursor = permissions_second_payload
                .page
                .next_cursor
                .clone()
                .expect("larger permissions second page must include next_cursor");
            let permissions_third_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=7&cursor={permissions_third_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger permissions third page request failed")?;
            let permissions_third_payload: ResourcePermissionsResponse =
                read_json(permissions_third_response).await?;
            let permissions_replay_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=7&cursor={permissions_cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("larger permissions replay page request failed")?;
            let permissions_replay_payload: ResourcePermissionsResponse =
                read_json(permissions_replay_response).await?;
            assert_replay_stable_pagination(
                &permissions_payload.data,
                &permissions_payload.page,
                &permissions_first_payload.data,
                &permissions_first_payload.page,
                &permissions_second_payload.data,
                &permissions_second_payload.page,
                &permissions_replay_payload.data,
                &permissions_replay_payload.page,
                "subject_scope_asc",
                50,
                7,
            );
            assert_page_walk_matches_base_prefix(
                &permissions_payload.data,
                &[
                    &permissions_first_payload.data,
                    &permissions_second_payload.data,
                    &permissions_third_payload.data,
                ],
            );
            assert_eq!(permissions_third_payload.page.next_cursor, None);

            database.cleanup().await?;
            Ok(())
        }
