        #[tokio::test]
        async fn address_history_contract_composes_current_and_historical_matches() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let current_resource_id = Uuid::from_u128(0xa240);
            let current_token_lineage_id = Uuid::from_u128(0xa241);
            let current_surface_binding_id = Uuid::from_u128(0xb240);
            let historical_resource_id = Uuid::from_u128(0xa242);
            let historical_token_lineage_id = Uuid::from_u128(0xa243);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x540", None, 540, 1_700_000_540),
                    raw_block(
                        "ethereum-mainnet",
                        "0x541",
                        Some("0x540"),
                        541,
                        1_700_000_541,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x542",
                        Some("0x541"),
                        542,
                        1_700_000_542,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x543",
                        Some("0x542"),
                        543,
                        1_700_000_543,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x544",
                        Some("0x543"),
                        544,
                        1_700_000_544,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x545",
                        Some("0x544"),
                        545,
                        1_700_000_545,
                    ),
                ],
            )
            .await
            .context("failed to upsert raw blocks for address-history conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(current_token_lineage_id, "0x540", 540),
                    address_name_token_lineage(historical_token_lineage_id, "0x541", 541),
                ],
            )
            .await
            .context("failed to upsert token lineages for address-history conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        current_resource_id,
                        Some(current_token_lineage_id),
                        "0x540",
                        540,
                    ),
                    address_name_resource(
                        historical_resource_id,
                        Some(historical_token_lineage_id),
                        "0x541",
                        541,
                    ),
                ],
            )
            .await
            .context("failed to upsert resources for address-history conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface("ens:current.eth", "current.eth", "node:current.eth", 540),
                    collection_name_surface(
                        "ens:historical.eth",
                        "historical.eth",
                        "node:historical.eth",
                        541,
                    ),
                ],
            )
            .await
            .context("failed to upsert name surfaces for address-history conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    current_surface_binding_id,
                    "ens:current.eth",
                    current_resource_id,
                    "0x540",
                    540,
                    1_717_173_540,
                )],
            )
            .await
            .context("failed to upsert surface bindings for address-history conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:current.eth",
                    bigname_storage::AddressNameRelation::Registrant,
                    "current.eth",
                    "current.eth",
                    "node:current.eth",
                    current_surface_binding_id,
                    current_resource_id,
                    Some(current_token_lineage_id),
                    540,
                )],
            )
            .await
            .context("failed to upsert current address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "current-surface",
                        Some("ens:current.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(544),
                        Some("0x544"),
                        Some("0xtx544"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-resource",
                        None,
                        Some(current_resource_id),
                        Some("ethereum-mainnet"),
                        Some(545),
                        Some("0x545"),
                        Some("0xtx545"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-surface",
                        Some("ens:historical.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(543),
                        Some("0x543"),
                        Some("0xtx543"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(542),
                        Some("0x542"),
                        Some("0xtx542"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-match",
                        "ens",
                        "ens:historical.eth",
                        historical_resource_id,
                        "RegistrationGranted",
                        541,
                        "0x541",
                        json!({
                            "registrant": "0x0000000000000000000000000000000000000ABC",
                        }),
                    ),
                ],
            )
            .await
            .context("failed to upsert normalized events for address-history conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/history/addresses/{address}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history base request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "current-resource",
                    "current-surface",
                    "historical-surface",
                    "historical-resource",
                    "historical-match",
                ]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["normalized_events".to_owned()]
            );
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(payload.consistency, "head");

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/history/addresses/{address}?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("address history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn basenames_history_routes_read_back_canonical_rows() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000b0b";
            let current_logical_name_id = "basenames:alice.base.eth";
            let historical_logical_name_id = "basenames:legacy.base.eth";
            let current_resource_id = Uuid::from_u128(0xa245);
            let current_surface_binding_id = Uuid::from_u128(0xb245);
            let historical_resource_id = Uuid::from_u128(0xa246);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xb641", None, 641, 1_700_000_641),
                    raw_block("base-mainnet", "0xb642", Some("0xb641"), 642, 1_700_000_642),
                    raw_block("base-mainnet", "0xb643", Some("0xb642"), 643, 1_700_000_643),
                    raw_block("base-mainnet", "0xb644", Some("0xb643"), 644, 1_700_000_644),
                    raw_block("base-mainnet", "0xb645", Some("0xb644"), 645, 1_700_000_645),
                ],
            )
            .await
            .context("failed to upsert basenames history raw blocks for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    Resource {
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xb641".to_owned(),
                        block_number: 641,
                        ..address_name_resource(current_resource_id, None, "0xb641", 641)
                    },
                    Resource {
                        chain_id: "base-mainnet".to_owned(),
                        block_hash: "0xb642".to_owned(),
                        block_number: 642,
                        ..address_name_resource(historical_resource_id, None, "0xb642", 642)
                    },
                ],
            )
            .await
            .context("failed to upsert basenames history resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        current_logical_name_id,
                        "alice.base.eth",
                        "node:alice.base.eth",
                        641,
                    ),
                    collection_name_surface(
                        historical_logical_name_id,
                        "legacy.base.eth",
                        "node:legacy.base.eth",
                        642,
                    ),
                ],
            )
            .await
            .context("failed to upsert basenames history surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[SurfaceBinding {
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: "0xb641".to_owned(),
                    block_number: 641,
                    ..address_name_surface_binding(
                        current_surface_binding_id,
                        current_logical_name_id,
                        current_resource_id,
                        "0xb641",
                        641,
                        1_717_173_641,
                    )
                }],
            )
            .await
            .context("failed to upsert basenames history surface binding for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    current_logical_name_id,
                    bigname_storage::AddressNameRelation::Registrant,
                    "alice.base.eth",
                    "alice.base.eth",
                    "node:alice.base.eth",
                    current_surface_binding_id,
                    current_resource_id,
                    None,
                    641,
                )],
            )
            .await
            .context("failed to upsert basenames history address-name row for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        ..history_event(
                            "current-surface",
                            Some(current_logical_name_id),
                            None,
                            Some("base-mainnet"),
                            Some(644),
                            Some("0xb644"),
                            Some("0xtx644"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        ..history_event(
                            "current-resource",
                            None,
                            Some(current_resource_id),
                            Some("base-mainnet"),
                            Some(645),
                            Some("0xb645"),
                            Some("0xtx645"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        ..history_event(
                            "historical-surface",
                            Some(historical_logical_name_id),
                            None,
                            Some("base-mainnet"),
                            Some(643),
                            Some("0xb643"),
                            Some("0xtx643"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        ..history_event(
                            "historical-resource",
                            None,
                            Some(historical_resource_id),
                            Some("base-mainnet"),
                            Some(642),
                            Some("0xb642"),
                            Some("0xtx642"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        chain_id: Some("base-mainnet".to_owned()),
                        ..authority_history_event(
                            "historical-match",
                            "basenames",
                            historical_logical_name_id,
                            historical_resource_id,
                            "RegistrationGranted",
                            641,
                            "0xb641",
                            json!({
                                "registrant": "0x0000000000000000000000000000000000000B0B",
                            }),
                        )
                    },
                ],
            )
            .await
            .context("failed to upsert basenames history normalized events for conformance")?;

            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/basenames/alice.base.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames name history request failed")?;
            assert_eq!(name_response.status(), StatusCode::OK);
            let name_payload: HistoryResponse = read_json(name_response).await?;
            assert_eq!(
                history_event_identities(&name_payload),
                vec!["current-resource", "current-surface"]
            );
            assert_eq!(name_payload.declared_state, json!({}));
            assert_eq!(
                name_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                name_payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(
                name_payload.chain_positions["base"]["chain_id"],
                json!("base-mainnet")
            );

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/history/resources/{current_resource_id}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames resource history request failed")?;
            assert_eq!(resource_response.status(), StatusCode::OK);
            let resource_payload: HistoryResponse = read_json(resource_response).await?;
            assert_eq!(
                history_event_identities(&resource_payload),
                vec!["current-resource", "current-surface"]
            );
            assert_eq!(
                resource_payload.chain_positions["base"]["chain_id"],
                json!("base-mainnet")
            );

            let address_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?namespace=basenames&relation=registrant"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames address history request failed")?;
            assert_eq!(address_response.status(), StatusCode::OK);
            let address_payload: HistoryResponse = read_json(address_response).await?;
            assert_eq!(
                history_event_identities(&address_payload),
                vec![
                    "current-resource",
                    "current-surface",
                    "historical-surface",
                    "historical-resource",
                    "historical-match",
                ]
            );
            assert_eq!(address_payload.declared_state, json!({}));
            assert_eq!(
                address_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                address_payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(
                address_payload.chain_positions["base"]["chain_id"],
                json!("base-mainnet")
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn ensv2_history_routes_read_back_canonical_rows_and_address_anchors() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture = EnsV2HistoryFixture::new();
            fixture.seed(&database).await?;

            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/current-v2.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 name history request failed")?;
            assert_eq!(name_response.status(), StatusCode::OK);
            let name_payload: HistoryResponse = read_json(name_response).await?;
            assert_eq!(
                history_event_identities(&name_payload),
                vec!["ensv2-current-resource", "ensv2-current-surface"]
            );
            assert_eq!(name_payload.declared_state, json!({}));
            assert_eq!(name_payload.page.sort, "chain_position_desc");
            assert_eq!(name_payload.page.page_size, 50);
            assert_eq!(
                name_payload.coverage.source_classes_considered,
                vec!["normalized_events".to_owned()]
            );
            assert_eq!(
                name_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                name_payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(name_payload.consistency, "head");
            assert_eq!(
                name_payload.chain_positions["ethereum-sepolia"]["block_number"],
                json!(fixture.base_block_number + 7)
            );
            assert_eq!(
                name_payload.data[0]
                    .get("source_family")
                    .and_then(Value::as_str),
                Some("ens_v2_registry_l1")
            );

            let name_surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/current-v2.eth?scope=surface")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 name surface-scope history request failed")?;
            assert_eq!(name_surface_response.status(), StatusCode::OK);
            let name_surface_payload: HistoryResponse = read_json(name_surface_response).await?;
            assert_eq!(
                history_event_identities(&name_surface_payload),
                vec!["ensv2-current-surface"]
            );
            assert_eq!(
                name_surface_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested surface scope"
            );

            let name_resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/current-v2.eth?scope=resource")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 name resource-scope history request failed")?;
            assert_eq!(name_resource_response.status(), StatusCode::OK);
            let name_resource_payload: HistoryResponse = read_json(name_resource_response).await?;
            assert_eq!(
                history_event_identities(&name_resource_payload),
                vec!["ensv2-current-resource"]
            );
            assert_eq!(
                name_resource_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested resource scope"
            );

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/resources/{}",
                            fixture.current_resource_id
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 resource history request failed")?;
            assert_eq!(resource_response.status(), StatusCode::OK);
            let resource_payload: HistoryResponse = read_json(resource_response).await?;
            assert_eq!(
                history_event_identities(&resource_payload),
                vec!["ensv2-current-resource", "ensv2-current-surface"]
            );
            assert_eq!(resource_payload.declared_state, json!({}));
            assert_eq!(resource_payload.page.sort, "chain_position_desc");
            assert_eq!(
                resource_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                resource_payload.chain_positions["ethereum-sepolia"]["block_number"],
                json!(fixture.base_block_number + 7)
            );

            let resource_surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/resources/{}?scope=surface",
                            fixture.current_resource_id
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 resource surface-scope history request failed")?;
            assert_eq!(resource_surface_response.status(), StatusCode::OK);
            let resource_surface_payload: HistoryResponse =
                read_json(resource_surface_response).await?;
            assert_eq!(
                history_event_identities(&resource_surface_payload),
                vec!["ensv2-current-surface"]
            );

            let resource_resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/resources/{}?scope=resource",
                            fixture.current_resource_id
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 resource resource-scope history request failed")?;
            assert_eq!(resource_resource_response.status(), StatusCode::OK);
            let resource_resource_payload: HistoryResponse =
                read_json(resource_resource_response).await?;
            assert_eq!(
                history_event_identities(&resource_resource_payload),
                vec!["ensv2-current-resource"]
            );

            let address_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 registrant address history request failed")?;
            assert_eq!(address_response.status(), StatusCode::OK);
            let address_payload: HistoryResponse = read_json(address_response).await?;
            assert_eq!(
                history_event_identities(&address_payload),
                fixture.expected_registrant_history_event_identities()
            );
            assert_eq!(address_payload.declared_state, json!({}));
            assert_eq!(address_payload.page.sort, "chain_position_desc");
            assert_eq!(address_payload.page.page_size, 50);
            assert_eq!(
                address_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                address_payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("normalized_event_history")
            );
            assert_eq!(
                address_payload.chain_positions["ethereum-sepolia"]["block_number"],
                json!(fixture.base_block_number + 7)
            );
            assert!(
                !history_event_identities(&address_payload)
                    .iter()
                    .any(|event_identity| event_identity.contains("observed"))
            );

            let address_surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant&scope=surface",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 registrant address surface-scope history request failed")?;
            assert_eq!(address_surface_response.status(), StatusCode::OK);
            let address_surface_payload: HistoryResponse =
                read_json(address_surface_response).await?;
            assert_eq!(
                history_event_identities(&address_surface_payload),
                vec![
                    "ensv2-current-surface",
                    "ensv2-historical-surface",
                    "ensv2-historical-grant",
                    "ensv2-historical-authority",
                    "ensv2-pending-grant",
                    "ensv2-pending-surface",
                ]
            );
            assert_eq!(
                address_surface_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested surface scope"
            );

            let address_resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant&scope=resource",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 registrant address resource-scope history request failed")?;
            assert_eq!(address_resource_response.status(), StatusCode::OK);
            let address_resource_payload: HistoryResponse =
                read_json(address_resource_response).await?;
            assert_eq!(
                history_event_identities(&address_resource_payload),
                vec![
                    "ensv2-current-resource",
                    "ensv2-historical-resource",
                    "ensv2-historical-grant",
                    "ensv2-historical-authority",
                ]
            );
            assert_eq!(
                address_resource_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested resource scope"
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant&page_size=2",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 address history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("ENSv2 address history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant&page_size=2&cursor={cursor}",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 address history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=registrant&page_size=2&cursor={cursor}",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 address history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &address_payload.data,
                &address_payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                2,
            );

            let controller_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{}?namespace=ens&relation=effective_controller",
                            fixture.controller
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 effective-controller address history request failed")?;
            assert_eq!(controller_response.status(), StatusCode::OK);
            let controller_payload: HistoryResponse = read_json(controller_response).await?;
            assert_eq!(
                history_event_identities(&controller_payload),
                fixture.expected_effective_controller_history_event_identities()
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_history_contract_honors_namespace_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000def";
            let registrant_resource_id = Uuid::from_u128(0xa250);
            let registrant_token_lineage_id = Uuid::from_u128(0xa251);
            let registrant_surface_binding_id = Uuid::from_u128(0xb250);
            let controller_resource_id = Uuid::from_u128(0xa252);
            let controller_surface_binding_id = Uuid::from_u128(0xb252);
            let basenames_resource_id = Uuid::from_u128(0xa253);
            let basenames_surface_binding_id = Uuid::from_u128(0xb253);
            let historical_resource_id = Uuid::from_u128(0xa254);
            let historical_token_lineage_id = Uuid::from_u128(0xa255);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x560", None, 560, 1_700_000_560),
                    raw_block(
                        "ethereum-mainnet",
                        "0x561",
                        Some("0x560"),
                        561,
                        1_700_000_561,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x562",
                        Some("0x561"),
                        562,
                        1_700_000_562,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x563",
                        Some("0x562"),
                        563,
                        1_700_000_563,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x564",
                        Some("0x563"),
                        564,
                        1_700_000_564,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x565",
                        Some("0x564"),
                        565,
                        1_700_000_565,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x566",
                        Some("0x565"),
                        566,
                        1_700_000_566,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x567",
                        Some("0x566"),
                        567,
                        1_700_000_567,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered raw blocks for conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[
                    address_name_token_lineage(registrant_token_lineage_id, "0x560", 560),
                    address_name_token_lineage(historical_token_lineage_id, "0x561", 561),
                ],
            )
            .await
            .context("failed to upsert filtered token lineages for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        registrant_resource_id,
                        Some(registrant_token_lineage_id),
                        "0x560",
                        560,
                    ),
                    address_name_resource(controller_resource_id, None, "0x561", 561),
                    address_name_resource(basenames_resource_id, None, "0x566", 566),
                    address_name_resource(
                        historical_resource_id,
                        Some(historical_token_lineage_id),
                        "0x562",
                        562,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[
                    collection_name_surface(
                        "ens:current-registrant.eth",
                        "current-registrant.eth",
                        "node:current-registrant.eth",
                        560,
                    ),
                    collection_name_surface(
                        "ens:current-controller.eth",
                        "current-controller.eth",
                        "node:current-controller.eth",
                        561,
                    ),
                    collection_name_surface(
                        "basenames:filtered.base.eth",
                        "filtered.base.eth",
                        "node:filtered.base.eth",
                        566,
                    ),
                    collection_name_surface(
                        "ens:historical-registrant.eth",
                        "historical-registrant.eth",
                        "node:historical-registrant.eth",
                        562,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered name surfaces for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[
                    address_name_surface_binding(
                        registrant_surface_binding_id,
                        "ens:current-registrant.eth",
                        registrant_resource_id,
                        "0x560",
                        560,
                        1_717_173_560,
                    ),
                    address_name_surface_binding(
                        controller_surface_binding_id,
                        "ens:current-controller.eth",
                        controller_resource_id,
                        "0x561",
                        561,
                        1_717_173_561,
                    ),
                    address_name_surface_binding(
                        basenames_surface_binding_id,
                        "basenames:filtered.base.eth",
                        basenames_resource_id,
                        "0x566",
                        566,
                        1_717_173_566,
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
                        "ens:current-registrant.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "current-registrant.eth",
                        "current-registrant.eth",
                        "node:current-registrant.eth",
                        registrant_surface_binding_id,
                        registrant_resource_id,
                        Some(registrant_token_lineage_id),
                        560,
                    ),
                    address_name_current_row(
                        address,
                        "ens:current-controller.eth",
                        bigname_storage::AddressNameRelation::EffectiveController,
                        "current-controller.eth",
                        "current-controller.eth",
                        "node:current-controller.eth",
                        controller_surface_binding_id,
                        controller_resource_id,
                        None,
                        561,
                    ),
                    address_name_current_row(
                        address,
                        "basenames:filtered.base.eth",
                        bigname_storage::AddressNameRelation::Registrant,
                        "filtered.base.eth",
                        "filtered.base.eth",
                        "node:filtered.base.eth",
                        basenames_surface_binding_id,
                        basenames_resource_id,
                        None,
                        566,
                    ),
                ],
            )
            .await
            .context("failed to upsert filtered address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "historical-registrant-match-surface",
                        Some("ens:historical-registrant.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(562),
                        Some("0x562"),
                        Some("0xtx562"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-registrant-match-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(561),
                        Some("0x561"),
                        Some("0xtx561"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-registrant-match",
                        "ens",
                        "ens:historical-registrant.eth",
                        historical_resource_id,
                        "RegistrationGranted",
                        560,
                        "0x560",
                        json!({
                            "registrant": "0x0000000000000000000000000000000000000DEF",
                        }),
                    ),
                    history_event(
                        "current-registrant-surface",
                        Some("ens:current-registrant.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(564),
                        Some("0x564"),
                        Some("0xtx564"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-registrant-resource",
                        None,
                        Some(registrant_resource_id),
                        Some("ethereum-mainnet"),
                        Some(565),
                        Some("0x565"),
                        Some("0xtx565"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-surface",
                        Some("ens:current-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(566),
                        Some("0x566"),
                        Some("0xtx566"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-resource",
                        None,
                        Some(controller_resource_id),
                        Some("ethereum-mainnet"),
                        Some(567),
                        Some("0x567"),
                        Some("0xtx567"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    NormalizedEvent {
                        namespace: "basenames".to_owned(),
                        ..history_event(
                            "filtered-basenames",
                            Some("basenames:filtered.base.eth"),
                            Some(basenames_resource_id),
                            Some("ethereum-mainnet"),
                            Some(563),
                            Some("0x563"),
                            Some("0xtx563"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                ],
            )
            .await
            .context("failed to upsert filtered normalized events for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?namespace=ens&relation=registrant"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("filtered address history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "current-registrant-resource",
                    "current-registrant-surface",
                    "historical-registrant-match-surface",
                    "historical-registrant-match-resource",
                    "historical-registrant-match",
                ]
            );
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(payload.page.page_size, 50);
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn address_history_contract_honors_scope_and_relation_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000123";
            let current_resource_id = Uuid::from_u128(0xa260);
            let current_token_lineage_id = Uuid::from_u128(0xa261);
            let current_surface_binding_id = Uuid::from_u128(0xb260);
            let historical_resource_id = Uuid::from_u128(0xa262);

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x570", None, 570, 1_700_000_570),
                    raw_block(
                        "ethereum-mainnet",
                        "0x571",
                        Some("0x570"),
                        571,
                        1_700_000_571,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x572",
                        Some("0x571"),
                        572,
                        1_700_000_572,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x573",
                        Some("0x572"),
                        573,
                        1_700_000_573,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x574",
                        Some("0x573"),
                        574,
                        1_700_000_574,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x575",
                        Some("0x574"),
                        575,
                        1_700_000_575,
                    ),
                ],
            )
            .await
            .context("failed to upsert scope raw blocks for conformance")?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[address_name_token_lineage(
                    current_token_lineage_id,
                    "0x570",
                    570,
                )],
            )
            .await
            .context("failed to upsert scope token lineage for conformance")?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[
                    address_name_resource(
                        current_resource_id,
                        Some(current_token_lineage_id),
                        "0x570",
                        570,
                    ),
                    address_name_resource(historical_resource_id, None, "0x571", 571),
                ],
            )
            .await
            .context("failed to upsert scope resources for conformance")?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[collection_name_surface(
                    "ens:current-controller.eth",
                    "current-controller.eth",
                    "node:current-controller.eth",
                    570,
                )],
            )
            .await
            .context("failed to upsert scope name surface for conformance")?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[address_name_surface_binding(
                    current_surface_binding_id,
                    "ens:current-controller.eth",
                    current_resource_id,
                    "0x570",
                    570,
                    1_717_173_570,
                )],
            )
            .await
            .context("failed to upsert scope surface binding for conformance")?;
            bigname_storage::upsert_address_names_current_rows(
                &database.pool,
                &[address_name_current_row(
                    address,
                    "ens:current-controller.eth",
                    bigname_storage::AddressNameRelation::EffectiveController,
                    "current-controller.eth",
                    "current-controller.eth",
                    "node:current-controller.eth",
                    current_surface_binding_id,
                    current_resource_id,
                    Some(current_token_lineage_id),
                    570,
                )],
            )
            .await
            .context("failed to upsert scope address-name anchors for conformance")?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "current-controller-surface",
                        Some("ens:current-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(574),
                        Some("0x574"),
                        Some("0xtx574"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "current-controller-resource",
                        None,
                        Some(current_resource_id),
                        Some("ethereum-mainnet"),
                        Some(575),
                        Some("0x575"),
                        Some("0xtx575"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-controller-surface",
                        Some("ens:historical-controller.eth"),
                        None,
                        Some("ethereum-mainnet"),
                        Some(573),
                        Some("0x573"),
                        Some("0xtx573"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "historical-controller-resource",
                        None,
                        Some(historical_resource_id),
                        Some("ethereum-mainnet"),
                        Some(572),
                        Some("0x572"),
                        Some("0xtx572"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    authority_history_event(
                        "historical-controller-match",
                        "ens",
                        "ens:historical-controller.eth",
                        historical_resource_id,
                        "AuthorityTransferred",
                        571,
                        "0x571",
                        json!({
                            "owner": "0x0000000000000000000000000000000000000123",
                        }),
                    ),
                ],
            )
            .await
            .context("failed to upsert scope normalized events for conformance")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=surface"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history surface-scope request failed")?;
            assert_eq!(surface_response.status(), StatusCode::OK);

            let surface_payload: HistoryResponse = read_json(surface_response).await?;
            assert_eq!(
                history_event_identities(&surface_payload),
                vec![
                    "current-controller-surface",
                    "historical-controller-surface",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                surface_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested surface scope"
            );

            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=resource"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history resource-scope request failed")?;
            assert_eq!(resource_response.status(), StatusCode::OK);

            let resource_payload: HistoryResponse = read_json(resource_response).await?;
            assert_eq!(
                history_event_identities(&resource_payload),
                vec![
                    "current-controller-resource",
                    "historical-controller-resource",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                resource_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested resource scope"
            );

            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/addresses/{address}?relation=effective_controller&scope=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("address history combined request failed")?;
            assert_eq!(both_response.status(), StatusCode::OK);

            let both_payload: HistoryResponse = read_json(both_response).await?;
            assert_eq!(
                history_event_identities(&both_payload),
                vec![
                    "current-controller-resource",
                    "current-controller-surface",
                    "historical-controller-surface",
                    "historical-controller-resource",
                    "historical-controller-match",
                ]
            );
            assert_eq!(
                both_payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_history_contract_returns_declared_rows_with_empty_declared_state() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa001);
            let surface_binding_id = Uuid::from_u128(0xb001);
            let manifest_id_v7 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap",
                    7,
                    "active",
                    "history-test-v1",
                )
                .await?;
            let manifest_id_v8 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap-next",
                    8,
                    "active",
                    "history-test-v2",
                )
                .await?;

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
                    raw_block(
                        "ethereum-mainnet",
                        "0x101",
                        Some("0x100"),
                        101,
                        1_700_000_101,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x102",
                        Some("0x101"),
                        102,
                        1_700_000_102,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x103",
                        Some("0x102"),
                        103,
                        1_700_000_103,
                    ),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:canonical",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(100),
                            Some("0x100"),
                            Some("0xtx100"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 8,
                        source_manifest_id: Some(manifest_id_v8),
                        ..history_event(
                            "history:safe",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(101),
                            Some("0x101"),
                            Some("0xtx101"),
                            Some(0),
                            CanonicalityState::Safe,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:finalized",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(102),
                            Some("0x102"),
                            Some("0xtx102"),
                            Some(0),
                            CanonicalityState::Finalized,
                        )
                    },
                    history_event(
                        "history:observed",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(103),
                        Some("0x103"),
                        Some("0xtx103"),
                        Some(0),
                        CanonicalityState::Observed,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec!["history:finalized", "history:safe", "history:canonical"]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                payload
                    .provenance
                    .get("manifest_versions")
                    .and_then(Value::as_array)
                    .map(Vec::len),
                Some(2)
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/alice.eth?page_size=1")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("name history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/ens/alice.eth?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_history_contract_returns_declared_rows_with_empty_declared_state() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa300);
            let surface_binding_id = Uuid::from_u128(0xb300);

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
                    raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
                    raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "no-chain-position",
                        Some(logical_name_id),
                        Some(resource_id),
                        None,
                        None,
                        None,
                        None,
                        None,
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "ethereum-higher-log",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(100),
                        Some("0xe100"),
                        Some("0xtx100"),
                        Some(7),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-same-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(100),
                        Some("0xb100"),
                        Some("0xtx090"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-higher-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(101),
                        Some("0xb101"),
                        Some("0xtx101"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!("/v1/history/resources/{resource_id}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "base-higher-height",
                    "base-same-height",
                    "ethereum-higher-log",
                    "no-chain-position",
                ]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.chain_positions,
                json!({
                    "base": {
                        "chain_id": "base-mainnet",
                        "block_number": 101,
                        "block_hash": "0xb101",
                        "timestamp": "2023-11-14T22:20:01Z"
                    },
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 100,
                        "block_hash": "0xe100",
                        "timestamp": "2023-11-14T22:20:00Z"
                    }
                })
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!("/v1/history/resources/{resource_id}?page_size=1"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: HistoryResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("resource history first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!(
                            "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: HistoryResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!(
                            "/v1/history/resources/{resource_id}?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: HistoryResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "chain_position_desc",
                50,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .context("failed to read conformance response body")?;
            serde_json::from_slice(&bytes).context("failed to decode conformance response JSON")
        }
