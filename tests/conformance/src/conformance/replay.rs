        struct ReplayCorpus {
            logical_name_id: &'static str,
            route_name: &'static str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
            address_names_address: &'static str,
            resolver_chain_id: &'static str,
            resolver_address: &'static str,
            primary_name_address: &'static str,
        }

        struct ReplayRoute {
            label: &'static str,
            uri: String,
        }

        pub(crate) async fn run_replay_capability_conformance() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_replay_supported_read_corpus(&database).await?;

            let before_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            replay_all_current_projections(&database).await?;
            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;

            assert_eq!(before_replay.len(), after_replay.len());
            for ((before_label, before_payload), (after_label, after_payload)) in
                before_replay.iter().zip(after_replay.iter())
            {
                assert_eq!(before_label, after_label);
                assert_eq!(
                    after_payload, before_payload,
                    "route payload changed after all-current-projections replay for {before_label}"
                );
            }

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_replay_supported_read_corpus(database: &HarnessDatabase) -> Result<ReplayCorpus> {
            let corpus = ReplayCorpus {
                logical_name_id: "basenames:alice.base.eth",
                route_name: "alice.base.eth",
                resource_id: Uuid::from_u128(0xc910),
                token_lineage_id: Uuid::from_u128(0xc911),
                surface_binding_id: Uuid::from_u128(0xc912),
                address_names_address: "0x00000000000000000000000000000000000000aa",
                resolver_chain_id: "base-mainnet",
                resolver_address: "0x0000000000000000000000000000000000000abc",
                primary_name_address: "0x0000000000000000000000000000000000000bcd",
            };

            seed_basenames_resolution_rebuild_inputs(
                database,
                corpus.logical_name_id,
                corpus.resource_id,
                corpus.token_lineage_id,
                corpus.surface_binding_id,
            )
            .await?;
            seed_replay_permissions(database, &corpus).await?;

            let child_fixture = EnsV2DeclaredChildFixture::new(
                "ens:parent.eth",
                "ens:alice.parent.eth",
                Uuid::from_u128(0xc920),
                Uuid::from_u128(0xc921),
                90,
            );
            child_fixture.seed(database).await?;

            database
                .seed_basenames_primary_name_claim_observation(
                    corpus.primary_name_address,
                    "60",
                    "Alice.base.eth",
                )
                .await?;

            database.rebuild_name_current(corpus.logical_name_id).await?;
            rebuild_children_current(database, None).await?;
            rebuild_record_inventory_current(database, corpus.resource_id).await?;
            rebuild_permissions_current(database, None).await?;
            rebuild_resolver_current(database, None, None).await?;
            rebuild_address_names_current(database, None).await?;
            database
                .rebuild_primary_names_current(corpus.primary_name_address, "basenames", "60")
                .await?;
            seed_replay_primary_name_execution(database, &corpus).await?;

            Ok(corpus)
        }

        async fn seed_replay_permissions(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let subject = "0x00000000000000000000000000000000000000bb";

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xreplay-permission-1", None, 106, 1_717_181_706),
                    raw_block("base-mainnet", "0xreplay-permission-2", None, 107, 1_717_181_707),
                ],
            )
            .await
            .context("failed to upsert replay permission raw blocks")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resource-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(106),
                        block_hash: Some("0xreplay-permission-1".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission1".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resource-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resource",
                            },
                            "effective_powers": ["resource_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resource-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resolver-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission2".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resolver-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resolver",
                                "chain_id": corpus.resolver_chain_id,
                                "resolver_address": corpus.resolver_address,
                            },
                            "effective_powers": ["resolver_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resolver-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                ],
            )
            .await
            .context("failed to upsert replay permission normalized events")?;

            Ok(())
        }

        async fn seed_replay_primary_name_execution(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000c91);
            let finished_at = timestamp(1_717_172_410);
            let verified_primary_name = json!({
                "status": "success",
                "name": {
                    "logical_name_id": corpus.logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": corpus.route_name,
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "namehash:alice.base.eth",
                    "resource_id": corpus.resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            });

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await
            .context("failed to seed replay primary-name execution trace")?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    "60",
                    verified_primary_name,
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await
            .context("failed to seed replay primary-name execution outcome")?;

            Ok(())
        }

        async fn snapshot_replay_supported_read_routes(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<Vec<(&'static str, Value)>> {
            let mut snapshots = Vec::new();
            for route in replay_supported_read_routes(corpus) {
                let payload = request_replay_route(database, &route).await?;
                snapshots.push((route.label, payload));
            }

            Ok(snapshots)
        }

        fn replay_supported_read_routes(corpus: &ReplayCorpus) -> Vec<ReplayRoute> {
            vec![
                ReplayRoute {
                    label: "exact-name",
                    uri: format!("/v1/names/basenames/{}", corpus.route_name),
                },
                ReplayRoute {
                    label: "children-collection",
                    uri: "/v1/names/ens/parent.eth/children".to_owned(),
                },
                ReplayRoute {
                    label: "address-names-collection",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.address_names_address
                    ),
                },
                ReplayRoute {
                    label: "name-history",
                    uri: format!(
                        "/v1/history/names/basenames/{}?scope=both",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "resource-history",
                    uri: format!("/v1/history/resources/{}?scope=both", corpus.resource_id),
                },
                ReplayRoute {
                    label: "address-history",
                    uri: format!(
                        "/v1/history/addresses/{}?namespace=basenames&relation=registrant",
                        corpus.address_names_address
                    ),
                },
                ReplayRoute {
                    label: "resolution",
                    uri: format!(
                        "/v1/resolutions/basenames/{}?mode=declared&records=addr:60,text",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "permissions",
                    uri: format!("/v1/resources/{}/permissions", corpus.resource_id),
                },
                ReplayRoute {
                    label: "resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.resolver_address
                    ),
                },
                ReplayRoute {
                    label: "primary-name",
                    uri: format!(
                        "/v1/primary-names/{}?namespace=basenames&coin_type=60&mode=both",
                        corpus.primary_name_address
                    ),
                },
            ]
        }

        async fn request_replay_route(
            database: &HarnessDatabase,
            route: &ReplayRoute,
        ) -> Result<Value> {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(route.uri.as_str())
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{} replay route request failed", route.label))?;

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{} replay route returned unexpected status",
                route.label
            );

            read_json(response)
                .await
                .with_context(|| format!("failed to decode {} replay route response", route.label))
        }
