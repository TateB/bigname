        #[tokio::test]
        async fn resolution_contract_returns_declared_and_verified_sections_by_mode() -> Result<()> {
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
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let default_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("default resolution request failed")?;
            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=declared&records=text:com.twitter,addr:60,avatar",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared resolution request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified resolution request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed")?;

            assert_eq!(default_response.status(), StatusCode::OK);
            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let default_payload: ResolutionResponse = read_json(default_response).await?;
            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let verified_payload: ResolutionResponse = read_json(verified_response).await?;
            let both_payload: ResolutionResponse = read_json(both_response).await?;
            let expected_default_declared_state =
                resolution_supported_declared_state(logical_name_id, resource_id, &["addr:60", "avatar"]);
            let expected_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["text:com.twitter", "addr:60", "avatar"],
            );
            let expected_both_declared_state =
                resolution_supported_declared_state(logical_name_id, resource_id, &["text:com.twitter"]);

            assert_eq!(
                default_payload.declared_state.as_ref(),
                Some(&expected_default_declared_state)
            );
            assert_eq!(default_payload.verified_state, None);
            assert_eq!(
                declared_payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(resolution_unsupported_verified_state(&["text", "addr:60"]))
            );
            assert_eq!(
                both_payload.declared_state.as_ref(),
                Some(&expected_both_declared_state)
            );
            assert_eq!(
                both_payload.verified_state,
                Some(resolution_unsupported_verified_state(&["text:com.twitter"]))
            );

            let default_declared_state = default_payload
                .declared_state
                .as_ref()
                .expect("default declared_state must be present");
            let inventory_selector_tuples = default_declared_state
                .get("record_inventory")
                .and_then(|value| value.get("selectors"))
                .and_then(Value::as_array)
                .expect("supported record_inventory must expose selectors")
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();
            assert_eq!(
                inventory_selector_tuples,
                vec![
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                    (
                        "text:com.twitter".to_owned(),
                        "text".to_owned(),
                        Some("com.twitter".to_owned()),
                    ),
                ]
            );

            let inventory_selector_tuple_set = inventory_selector_tuples
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let topology_record_version_boundary = default_declared_state
                .get("topology")
                .and_then(|value| value.get("version_boundaries"))
                .and_then(|value| value.get("record_version_boundary"))
                .expect("supported topology must expose record_version_boundary");
            let record_inventory_version_boundary = default_declared_state
                .get("record_inventory")
                .and_then(|value| value.get("record_version_boundary"))
                .expect("supported record_inventory must expose record_version_boundary");
            let full_cache = default_declared_state
                .get("record_cache")
                .expect("supported record_cache must be present");
            let full_cache_entries = full_cache
                .get("entries")
                .and_then(Value::as_array)
                .expect("supported record_cache must expose entries");
            let full_cache_selector_tuples = full_cache_entries
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();

            assert_eq!(
                record_inventory_version_boundary,
                topology_record_version_boundary
            );
            assert_eq!(
                full_cache.get("record_version_boundary"),
                Some(topology_record_version_boundary)
            );
            assert_eq!(
                full_cache_selector_tuples,
                vec![
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                ]
            );
            assert!(
                full_cache_selector_tuples
                    .iter()
                    .all(|tuple| inventory_selector_tuple_set.contains(tuple))
            );

            let narrowed_cache_selector_tuples = declared_payload
                .declared_state
                .as_ref()
                .and_then(|value| value.get("record_cache"))
                .and_then(|value| value.get("entries"))
                .and_then(Value::as_array)
                .expect("declared mode record_cache must expose entries")
                .iter()
                .map(record_selector_identity_tuple)
                .collect::<Vec<_>>();
            assert_eq!(
                narrowed_cache_selector_tuples,
                vec![
                    (
                        "text:com.twitter".to_owned(),
                        "text".to_owned(),
                        Some("com.twitter".to_owned()),
                    ),
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned())
                    ),
                    ("avatar".to_owned(), "avatar".to_owned(), None),
                ]
            );
            assert!(
                narrowed_cache_selector_tuples
                    .iter()
                    .all(|tuple| inventory_selector_tuple_set.contains(tuple))
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn get_resolution_payload(
            database: &HarnessDatabase,
            uri: &str,
        ) -> Result<ResolutionResponse> {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("resolution request failed for {uri}"))?;

            assert_eq!(response.status(), StatusCode::OK, "uri {uri}");

            read_json(response).await
        }

        #[tokio::test]
        async fn resolution_inferred_route_matches_canonical_ens_for_exact_base_eth() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:base.eth";
            let resource_id = Uuid::from_u128(0x7e10);
            let token_lineage_id = Uuid::from_u128(0x7e11);
            let surface_binding_id = Uuid::from_u128(0x7e12);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let canonical_payload =
                get_resolution_payload(&database, "/v1/resolutions/ens/base.eth").await?;
            let inferred_payload = get_resolution_payload(&database, "/v1/resolve/base.eth").await?;

            assert_eq!(inferred_payload, canonical_payload);
            assert_eq!(
                inferred_payload.data.get("namespace"),
                Some(&json!("ens"))
            );
            assert_eq!(
                inferred_payload.data.get("logical_name_id"),
                Some(&json!("ens:base.eth"))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_inferred_route_matches_canonical_basenames_and_keeps_verified_selector_local()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let basenames_logical_name_id = "basenames:alice.base.eth";
            let basenames_resource_id = Uuid::from_u128(0x7b10);
            let basenames_token_lineage_id = Uuid::from_u128(0x7b11);
            let basenames_surface_binding_id = Uuid::from_u128(0x7b12);
            let ens_logical_name_id = "ens:alice.base.eth";
            let ens_resource_id = Uuid::from_u128(0x7e20);
            let ens_token_lineage_id = Uuid::from_u128(0x7e21);
            let ens_surface_binding_id = Uuid::from_u128(0x7e22);
            let ens_execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
            let records_query = "text:com.twitter,addr:60";

            seed_basenames_resolution_rebuild_inputs(
                &database,
                basenames_logical_name_id,
                basenames_resource_id,
                basenames_token_lineage_id,
                basenames_surface_binding_id,
            )
            .await?;
            database
                .rebuild_name_current(basenames_logical_name_id)
                .await?;
            rebuild_record_inventory_current(&database, basenames_resource_id).await?;

            database
                .seed_exact_name_rebuild_inputs(
                    ens_logical_name_id,
                    ens_resource_id,
                    ens_token_lineage_id,
                    ens_surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(ens_logical_name_id).await?;
            let ens_record_inventory_row =
                resolution_record_inventory_current_row(ens_logical_name_id, ens_resource_id);
            database
                .insert_record_inventory_current_row(ens_record_inventory_row.clone())
                .await?;
            let ens_name_row = bigname_storage::load_name_current(
                &database.pool,
                ens_logical_name_id,
            )
            .await?
            .context("ENS decoy row must exist for inferred basenames fallback guard")?;
            let ens_records = parse_resolution_record_keys(
                Some(records_query),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let ens_cache_key = build_resolution_execution_cache_key(
                &ens_name_row,
                &ens_records,
                Some(&ens_record_inventory_row),
            )?;
            let ens_request_key = ens_cache_key.request_key.clone();
            let ens_verified_queries =
                resolution_execution_verified_queries(ens_execution_trace_id, &[
                    "text:com.twitter",
                    "addr:60",
                ]);

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    ens_execution_trace_id,
                    &ens_request_key,
                    &["text:com.twitter", "addr:60"],
                    ens_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    ens_execution_trace_id,
                    ens_cache_key,
                    ens_verified_queries,
                ),
            )
            .await?;

            let canonical_declared_payload = get_resolution_payload(
                &database,
                "/v1/resolutions/basenames/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
            )
            .await?;
            let inferred_declared_payload = get_resolution_payload(
                &database,
                "/v1/resolve/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
            )
            .await?;
            let canonical_verified_payload = get_resolution_payload(
                &database,
                "/v1/resolutions/basenames/alice.base.eth?mode=verified&records=text:com.twitter,addr:60",
            )
            .await?;
            let inferred_verified_payload = get_resolution_payload(
                &database,
                "/v1/resolve/alice.base.eth?mode=verified&records=text:com.twitter,addr:60",
            )
            .await?;

            assert_eq!(inferred_declared_payload, canonical_declared_payload);
            assert_eq!(inferred_verified_payload, canonical_verified_payload);
            assert_eq!(
                inferred_declared_payload.data.get("namespace"),
                Some(&json!("basenames"))
            );
            assert_eq!(
                inferred_declared_payload.data.get("logical_name_id"),
                Some(&json!("basenames:alice.base.eth"))
            );
            assert_eq!(
                inferred_verified_payload.data.get("namespace"),
                Some(&json!("basenames"))
            );
            assert_eq!(
                inferred_verified_payload.data.get("logical_name_id"),
                Some(&json!("basenames:alice.base.eth"))
            );
            assert_eq!(
                inferred_verified_payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );
            assert_eq!(
                inferred_verified_payload.provenance.get("execution_trace_id"),
                Some(&Value::Null)
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_requires_records_for_verified_modes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified resolution request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed")?;

            assert_eq!(verified_response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(both_response.status(), StatusCode::BAD_REQUEST);

            let verified_payload: ErrorResponse = read_json(verified_response).await?;
            let both_payload: ErrorResponse = read_json(both_response).await?;
            assert_eq!(verified_payload.error.code, "invalid_input");
            assert_eq!(both_payload.error.code, "invalid_input");
            assert_eq!(
                verified_payload.error.message,
                "records is required when mode is verified or both"
            );
            assert_eq!(both_payload.error.message, verified_payload.error.message);
            assert!(verified_payload.error.details.is_empty());
            assert!(both_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[derive(Clone, Copy, Debug)]
        enum BasenamesDeferredVerifiedPathCase {
            AliasParticipating,
            WildcardDerived,
            LinkedSubregistry,
            TransportFree,
            ReservedOffchainGateway,
        }

        impl BasenamesDeferredVerifiedPathCase {
            fn all() -> [Self; 5] {
                [
                    Self::AliasParticipating,
                    Self::WildcardDerived,
                    Self::LinkedSubregistry,
                    Self::TransportFree,
                    Self::ReservedOffchainGateway,
                ]
            }

            fn label(self) -> &'static str {
                match self {
                    Self::AliasParticipating => "alias_participating",
                    Self::WildcardDerived => "wildcard_derived",
                    Self::LinkedSubregistry => "linked_subregistry",
                    Self::TransportFree => "transport_free",
                    Self::ReservedOffchainGateway => "reserved_offchain_gateway",
                }
            }
        }

        fn basenames_name_ref(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            resource_id: Uuid,
            binding_kind: &str,
        ) -> Value {
            json!({
                "logical_name_id": logical_name_id,
                "namespace": "basenames",
                "normalized_name": normalized_name,
                "canonical_display_name": canonical_display_name,
                "namehash": format!("namehash:{normalized_name}"),
                "resource_id": resource_id.to_string(),
                "binding_kind": binding_kind,
            })
        }

        fn basenames_resolver_hop(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            resource_id: Uuid,
        ) -> Value {
            json!({
                "logical_name_id": logical_name_id,
                "namespace": "basenames",
                "normalized_name": normalized_name,
                "canonical_display_name": canonical_display_name,
                "resource_id": resource_id.to_string(),
                "chain_id": "base-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged",
            })
        }

        fn basenames_supported_topology(
            logical_name_id: &str,
            resource_id: Uuid,
            record_version_boundary: &Value,
        ) -> Value {
            json!({
                "registry_path": [basenames_name_ref(
                    logical_name_id,
                    "alice.base.eth",
                    "Alice.base.eth",
                    resource_id,
                    "declared_registry_path",
                )],
                "subregistry_path": [],
                "resolver_path": [basenames_resolver_hop(
                    logical_name_id,
                    "alice.base.eth",
                    "Alice.base.eth",
                    resource_id,
                )],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": record_version_boundary.clone(),
                    "record_version_boundary": record_version_boundary.clone(),
                },
                "transport": {
                    "source_chain_id": "base-mainnet",
                    "target_chain_id": "ethereum-mainnet",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                    "latest_event_kind": null,
                },
            })
        }

        fn basenames_deferred_verified_path_topology(
            case: BasenamesDeferredVerifiedPathCase,
            logical_name_id: &str,
            resource_id: Uuid,
            record_version_boundary: &Value,
        ) -> Value {
            let mut topology =
                basenames_supported_topology(logical_name_id, resource_id, record_version_boundary);

            match case {
                BasenamesDeferredVerifiedPathCase::AliasParticipating => {
                    let alias_target = basenames_name_ref(
                        "basenames:resolver.base.eth",
                        "resolver.base.eth",
                        "Resolver.base.eth",
                        Uuid::from_u128(0x7201),
                        "resolver_alias_path",
                    );
                    topology["alias"] = json!({
                        "final_target": alias_target.clone(),
                        "hops": [alias_target],
                    });
                }
                BasenamesDeferredVerifiedPathCase::WildcardDerived => {
                    let wildcard_source = basenames_name_ref(
                        "basenames:wild.base.eth",
                        "wild.base.eth",
                        "Wild.base.eth",
                        Uuid::from_u128(0x7202),
                        "observed_wildcard_path",
                    );
                    topology["resolver_path"] = json!([basenames_resolver_hop(
                        "basenames:wild.base.eth",
                        "wild.base.eth",
                        "Wild.base.eth",
                        Uuid::from_u128(0x7202),
                    )]);
                    topology["wildcard"] = json!({
                        "source": wildcard_source,
                        "matched_labels": ["alice"],
                    });
                }
                BasenamesDeferredVerifiedPathCase::LinkedSubregistry => {
                    topology["subregistry_path"] = json!([basenames_name_ref(
                        "basenames:child.base.eth",
                        "child.base.eth",
                        "Child.base.eth",
                        Uuid::from_u128(0x7203),
                        "linked_subregistry_path",
                    )]);
                }
                BasenamesDeferredVerifiedPathCase::TransportFree => {
                    topology["transport"] = json!({
                        "source_chain_id": null,
                        "target_chain_id": null,
                        "contract_address": null,
                        "latest_event_kind": null,
                    });
                }
                BasenamesDeferredVerifiedPathCase::ReservedOffchainGateway => {
                    topology["transport"]["gateway"] = json!("https://basenames.example.test");
                }
            }

            topology
        }

        async fn assert_basenames_deferred_verified_path_case_stays_selector_local(
            case: BasenamesDeferredVerifiedPathCase,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x7210);
            let token_lineage_id = Uuid::from_u128(0x7211);
            let surface_binding_id = Uuid::from_u128(0x7212);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000003a);
            let request_key = basenames_resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["text:com.twitter", "addr:60"]);

            seed_basenames_resolution_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
            database.rebuild_name_current(logical_name_id).await?;
            rebuild_record_inventory_current(&database, resource_id).await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "basenames declared resolution request failed before {} assertions",
                        case.label()
                    )
                })?;
            assert_eq!(
                declared_response.status(),
                StatusCode::OK,
                "case {}",
                case.label()
            );

            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let record_inventory_boundary = declared_payload
                .declared_state
                .as_ref()
                .and_then(|state| state.get("record_inventory"))
                .and_then(|value| value.get("record_version_boundary"))
                .cloned()
                .context("basenames declared resolution must include record_inventory boundary")?;
            let worker_row = bigname_storage::load_record_inventory_current(
                &database.pool,
                resource_id,
                &record_inventory_boundary,
            )
            .await?
            .context("worker-produced basenames record_inventory_current row must exist")?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("basenames deferred verified path test requires name_current row")?;
            append_basenames_execution_manifest_version(&mut name_row);
            insert_basenames_supported_ethereum_position(&mut name_row);
            let topology = basenames_deferred_verified_path_topology(
                case,
                logical_name_id,
                resource_id,
                &worker_row.record_version_boundary,
            );
            name_row.declared_summary["topology"] = topology.clone();
            database.insert_name_current_row(name_row.clone()).await?;
            let requested_chain_positions =
                requested_chain_positions_from_name_current(&name_row.chain_positions);

            upsert_execution_trace(
                &database.pool,
                &basenames_resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["text:com.twitter", "addr:60"],
                    requested_chain_positions.clone(),
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &basenames_resolution_execution_outcome(
                    execution_trace_id,
                    &request_key,
                    requested_chain_positions,
                    name_row
                        .provenance
                        .get("manifest_versions")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                    worker_row.record_version_boundary.clone(),
                    persisted_verified_queries,
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "deferred basenames mixed resolution request failed for {}",
                        case.label()
                    )
                })?;
            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "deferred basenames execution explain request failed for {}",
                        case.label()
                    )
                })?;

            assert_eq!(response.status(), StatusCode::OK, "case {}", case.label());
            assert_eq!(
                explain_response.status(),
                StatusCode::NOT_FOUND,
                "case {}",
                case.label()
            );

            let payload: ResolutionResponse = read_json(response).await?;
            assert_eq!(
                payload
                    .declared_state
                    .as_ref()
                    .and_then(|state| state.get("topology")),
                Some(&topology),
                "case {}",
                case.label()
            );
            assert_eq!(
                payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ])),
                "case {}",
                case.label()
            );
            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::Null),
                "case {}",
                case.label()
            );

            let explain_payload: ErrorResponse = read_json(explain_response).await?;
            assert_eq!(
                explain_payload.error.code,
                "not_found",
                "case {}",
                case.label()
            );
            assert_eq!(
                explain_payload.error.message,
                "persisted resolution execution explain was not found for name alice.base.eth in namespace basenames",
                "case {}",
                case.label()
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_persisted_basenames_transport_direct_answers() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x7200);
            let token_lineage_id = Uuid::from_u128(0x7100);
            let surface_binding_id = Uuid::from_u128(0x7300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000034);
            let request_key = basenames_resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["text:com.twitter", "addr:60"]);

            seed_basenames_resolution_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
            database.rebuild_name_current(logical_name_id).await?;
            rebuild_record_inventory_current(&database, resource_id).await?;
            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames declared resolution request failed before seeding execution")?;
            assert_eq!(declared_response.status(), StatusCode::OK);
            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let record_inventory_boundary = declared_payload
                .declared_state
                .as_ref()
                .and_then(|state| state.get("record_inventory"))
                .and_then(|value| value.get("record_version_boundary"))
                .cloned()
                .context("basenames declared resolution must include record_inventory boundary")?;
            let worker_row = bigname_storage::load_record_inventory_current(
                &database.pool,
                resource_id,
                &record_inventory_boundary,
            )
            .await?
            .context("worker-produced basenames record_inventory_current row must exist")?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("basenames supported resolution test requires name_current row")?;
            let topology = json!({
                "registry_path": [
                    {
                        "logical_name_id": logical_name_id,
                        "namespace": "basenames",
                        "normalized_name": "alice.base.eth",
                        "canonical_display_name": "Alice.base.eth",
                        "namehash": "namehash:alice.base.eth",
                        "resource_id": resource_id.to_string(),
                        "binding_kind": "declared_registry_path",
                    }
                ],
                "subregistry_path": [],
                "resolver_path": [
                    {
                        "logical_name_id": logical_name_id,
                        "namespace": "basenames",
                        "normalized_name": "alice.base.eth",
                        "canonical_display_name": "Alice.base.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "base-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": worker_row.record_version_boundary.clone(),
                    "record_version_boundary": worker_row.record_version_boundary.clone(),
                },
                "transport": {
                    "source_chain_id": "base-mainnet",
                    "target_chain_id": "ethereum-mainnet",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                    "latest_event_kind": null,
                },
            });
            append_basenames_execution_manifest_version(&mut name_row);
            insert_basenames_supported_ethereum_position(&mut name_row);
            name_row.declared_summary["topology"] = topology.clone();
            database.insert_name_current_row(name_row.clone()).await?;
            let requested_chain_positions =
                requested_chain_positions_from_name_current(&name_row.chain_positions);

            upsert_execution_trace(
                &database.pool,
                &basenames_resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["text:com.twitter", "addr:60"],
                    requested_chain_positions.clone(),
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &basenames_resolution_execution_outcome(
                    execution_trace_id,
                    &request_key,
                    requested_chain_positions,
                    name_row
                        .provenance
                        .get("manifest_versions")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                    worker_row.record_version_boundary.clone(),
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames mixed resolution request failed")?;
            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames resolution execution explain request failed")?;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(explain_response.status(), StatusCode::OK);

            let payload: ResolutionResponse = read_json(response).await?;
            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let declared_state = payload
                .declared_state
                .as_ref()
                .context("basenames mixed resolution must include declared_state")?;

            assert_eq!(declared_state.get("topology"), Some(&topology));
            assert_eq!(
                declared_state.get("record_inventory"),
                Some(&json!({
                    "record_version_boundary": worker_row.record_version_boundary.clone(),
                    "enumeration_basis": worker_row.enumeration_basis.clone(),
                    "selectors": worker_row.selectors.clone(),
                    "explicit_gaps": worker_row.explicit_gaps.clone(),
                    "unsupported_families": worker_row.unsupported_families.clone(),
                    "last_change": worker_row.last_change.clone().unwrap_or(Value::Null),
                }))
            );
            assert_eq!(
                declared_state.get("record_cache"),
                Some(&json!({
                    "record_version_boundary": worker_row.record_version_boundary.clone(),
                    "entries": [
                        {
                            "record_key": "text:com.twitter",
                            "record_family": "text",
                            "selector_key": "com.twitter",
                            "status": "not_found",
                        },
                        {
                            "record_key": "addr:60",
                            "record_family": "addr",
                            "selector_key": "60",
                            "status": "unsupported",
                            "unsupported_reason": "value_not_retained_in_normalized_events",
                        }
                    ],
                }))
            );
            assert_eq!(
                payload.verified_state,
                Some(json!({
                    "verified_queries": [
                        {
                            "record_key": "text:com.twitter",
                            "status": "not_found",
                            "failure_reason": "no_text_record",
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        },
                        {
                            "record_key": "addr:60",
                            "status": "success",
                            "value": {
                                "coin_type": "60",
                                "value": "0x00000000000000000000000000000000000000aa",
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }
                    ]
                }))
            );
            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(
                payload.provenance.get("manifest_versions"),
                name_row.provenance.get("manifest_versions")
            );
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": basenames_resolution_execution_summary(
                        execution_trace_id,
                        logical_name_id,
                        resource_id,
                    ),
                    "verified_queries": payload
                        .verified_state
                        .as_ref()
                        .and_then(|state| state.get("verified_queries"))
                        .cloned()
                        .expect("verified_state must include verified_queries"),
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_keeps_basenames_transport_explicit_without_projected_topology()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x7403);
            let token_lineage_id = Uuid::from_u128(0x7404);
            let surface_binding_id = Uuid::from_u128(0x7405);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000037);
            let request_key = basenames_resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["text:com.twitter", "addr:60"]);

            seed_basenames_resolution_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
            database.rebuild_name_current(logical_name_id).await?;
            rebuild_record_inventory_current(&database, resource_id).await?;
            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames declared resolution request failed before missing-topology conformance assertions")?;
            assert_eq!(declared_response.status(), StatusCode::OK);
            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let record_inventory_boundary = declared_payload
                .declared_state
                .as_ref()
                .and_then(|state| state.get("record_inventory"))
                .and_then(|value| value.get("record_version_boundary"))
                .cloned()
                .context("basenames declared resolution must include record_inventory boundary")?;
            let worker_row = bigname_storage::load_record_inventory_current(
                &database.pool,
                resource_id,
                &record_inventory_boundary,
            )
            .await?
            .context("worker-produced basenames record_inventory_current row must exist")?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("basenames missing-topology conformance test requires name_current row")?;
            append_basenames_execution_manifest_version(&mut name_row);
            insert_basenames_supported_ethereum_position(&mut name_row);
            database.insert_name_current_row(name_row.clone()).await?;
            let requested_chain_positions =
                requested_chain_positions_from_name_current(&name_row.chain_positions);

            upsert_execution_trace(
                &database.pool,
                &basenames_resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["text:com.twitter", "addr:60"],
                    requested_chain_positions.clone(),
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &basenames_resolution_execution_outcome(
                    execution_trace_id,
                    &request_key,
                    requested_chain_positions,
                    name_row
                        .provenance
                        .get("manifest_versions")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                    worker_row.record_version_boundary.clone(),
                    persisted_verified_queries,
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("missing-topology basenames mixed resolution request failed")?;
            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("missing-topology basenames execution explain request failed")?;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(explain_response.status(), StatusCode::NOT_FOUND);

            let payload: ResolutionResponse = read_json(response).await?;
            assert_eq!(
                payload.verified_state,
                Some(json!({
                    "verified_queries": [
                        {
                            "record_key": "text:com.twitter",
                            "status": "unsupported",
                            "unsupported_reason": "verified resolution entrypoint is not yet supported",
                        },
                        {
                            "record_key": "addr:60",
                            "status": "unsupported",
                            "unsupported_reason": "verified resolution entrypoint is not yet supported",
                        }
                    ]
                }))
            );
            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::Null)
            );

            let explain_payload: ErrorResponse = read_json(explain_response).await?;
            assert_eq!(explain_payload.error.code, "not_found");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_keeps_out_of_class_basenames_transport_explicit() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x7400);
            let token_lineage_id = Uuid::from_u128(0x7401);
            let surface_binding_id = Uuid::from_u128(0x7402);

            seed_basenames_resolution_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
            database.rebuild_name_current(logical_name_id).await?;
            rebuild_record_inventory_current(&database, resource_id).await?;
            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=declared&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames declared resolution request failed before negative assertions")?;
            assert_eq!(declared_response.status(), StatusCode::OK);
            let declared_payload: ResolutionResponse = read_json(declared_response).await?;
            let record_inventory_boundary = declared_payload
                .declared_state
                .as_ref()
                .and_then(|state| state.get("record_inventory"))
                .and_then(|value| value.get("record_version_boundary"))
                .cloned()
                .context("basenames declared resolution must include record_inventory boundary")?;
            let worker_row = bigname_storage::load_record_inventory_current(
                &database.pool,
                resource_id,
                &record_inventory_boundary,
            )
            .await?
            .context("worker-produced basenames record_inventory_current row must exist")?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("basenames negative resolution test requires name_current row")?;
            append_basenames_execution_manifest_version(&mut name_row);
            insert_basenames_supported_ethereum_position(&mut name_row);
            name_row.declared_summary["topology"] = json!({
                "registry_path": [{
                    "logical_name_id": logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "namehash:alice.base.eth",
                    "resource_id": resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }],
                "subregistry_path": [],
                "resolver_path": [{
                    "logical_name_id": logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "resource_id": resource_id.to_string(),
                    "chain_id": "base-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged",
                }],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": worker_row.record_version_boundary.clone(),
                    "record_version_boundary": worker_row.record_version_boundary.clone(),
                },
                "transport": {
                    "source_chain_id": "base-mainnet",
                    "target_chain_id": "ethereum-mainnet",
                    "contract_address": "0x0000000000000000000000000000000000000bad",
                    "latest_event_kind": null,
                },
            });
            database.insert_name_current_row(name_row).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/basenames/alice.base.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("out-of-class basenames mixed resolution request failed")?;
            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("out-of-class basenames resolution execution explain request failed")?;

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(explain_response.status(), StatusCode::NOT_FOUND);

            let payload: ResolutionResponse = read_json(response).await?;
            assert_eq!(
                payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );
            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::Null)
            );

            let explain_payload: ErrorResponse = read_json(explain_response).await?;
            assert_eq!(explain_payload.error.code, "not_found");
            assert_eq!(
                explain_payload.error.message,
                "persisted resolution execution explain was not found for name alice.base.eth in namespace basenames"
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_keeps_basenames_deferred_path_classes_selector_local() -> Result<()> {
            for case in BasenamesDeferredVerifiedPathCase::all() {
                assert_basenames_deferred_verified_path_case_stays_selector_local(case).await?;
            }

            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_rejects_duplicate_records_for_verified_modes() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,text")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("duplicate resolution request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "invalid_input");
            assert_eq!(
                payload.error.message,
                "records must not contain duplicate selectors"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_rejects_malformed_records() -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=:avatar")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed resolution request failed")?;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "invalid_input");
            assert_eq!(
                payload.error.message,
                "records must contain only valid record selectors"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reuses_exact_name_envelope_fields() -> Result<()> {
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
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name request failed")?;

            assert_eq!(resolution_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let expected_resolution_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["text:com.twitter", "addr:60"],
            );

            assert_eq!(resolution_payload.data, name_payload.data);
            assert_eq!(resolution_payload.provenance, name_payload.provenance);
            assert_eq!(resolution_payload.coverage, name_payload.coverage);
            assert_eq!(
                resolution_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(resolution_payload.consistency, name_payload.consistency);
            assert_eq!(resolution_payload.last_updated, name_payload.last_updated);
            assert_eq!(
                resolution_payload.declared_state.as_ref(),
                Some(&expected_resolution_declared_state)
            );
            assert_eq!(
                resolution_payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_persisted_avatar_answer_on_mixed_route_and_preserves_request_order()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000023);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("mixed contenthash resolution requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let persisted_records = parse_resolution_record_keys(
                Some("text:com.twitter,contenthash,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &persisted_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["avatar", "text:com.twitter", "contenthash", "addr:60"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["avatar", "text:com.twitter", "contenthash", "addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(execution_trace_id, cache_key, persisted_verified_queries),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed contenthash resolution request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolutionResponse = read_json(response).await?;
            let expected_declared_state = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["avatar", "text:com.twitter", "contenthash", "addr:60"],
            );
            let expected_verified_state = json!({
                "verified_queries": resolution_execution_verified_queries(
                    execution_trace_id,
                    &["avatar", "text:com.twitter", "contenthash", "addr:60"],
                ),
            });

            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(
                payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(payload.verified_state, Some(expected_verified_state));

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_reads_persisted_answer_and_reuses_resolution_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000021);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let explain_records =
                parse_resolution_record_keys(Some("text:com.twitter,addr:60"), ResolutionMode::Verified)
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &explain_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["addr:60", "text:com.twitter"]);

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60", "text:com.twitter"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(execution_trace_id, cache_key, persisted_verified_queries),
            )
            .await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed")?;
            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text:com.twitter,addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(resolution_response.status(), StatusCode::OK);

            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let expected_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["text:com.twitter", "addr:60"]);

            assert_eq!(explain_payload.data, resolution_payload.data);
            assert_eq!(explain_payload.coverage, resolution_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                resolution_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, resolution_payload.consistency);
            assert_eq!(
                explain_payload.last_updated,
                resolution_payload.last_updated
            );
            assert_eq!(
                explain_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(execution_trace_id, resource_id),
                    "verified_queries": expected_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_reads_persisted_avatar_answer_and_reuses_resolution_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000024);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let explain_records = parse_resolution_record_keys(
                Some("text:com.twitter,contenthash,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &explain_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["avatar", "text:com.twitter", "contenthash", "addr:60"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["avatar", "text:com.twitter", "contenthash", "addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(execution_trace_id, cache_key, persisted_verified_queries),
            )
            .await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("contenthash execution explain request failed")?;
            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,contenthash,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("contenthash verified resolution request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(resolution_response.status(), StatusCode::OK);

            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let expected_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["avatar", "text:com.twitter", "contenthash", "addr:60"],
            );

            assert_eq!(explain_payload.data, resolution_payload.data);
            assert_eq!(explain_payload.coverage, resolution_payload.coverage);
            assert_eq!(explain_payload.provenance, resolution_payload.provenance);
            assert_eq!(
                explain_payload.chain_positions,
                resolution_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, resolution_payload.consistency);
            assert_eq!(
                explain_payload.last_updated,
                resolution_payload.last_updated
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                resolution_payload.verified_state,
                Some(json!({
                    "verified_queries": expected_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(execution_trace_id, resource_id),
                    "verified_queries": expected_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_persisted_alias_only_avatar_answer_on_mixed_route() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000025);
            let alias_target = json!({
                "logical_name_id": "ens:profile.alice.eth",
                "namespace": "ens",
                "normalized_name": "profile.alice.eth",
                "canonical_display_name": "Profile.alice.eth",
                "namehash": "namehash:profile.alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "resolver_alias_path",
            });

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("mixed alias resolution requires an exact-name current row")?;
            name_row.binding_kind = Some(SurfaceBindingKind::ResolverAliasPath);
            database.insert_name_current_row(name_row.clone()).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let alias_records =
                parse_resolution_record_keys(Some("text:com.twitter"), ResolutionMode::Verified)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &alias_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries =
                resolution_alias_only_verified_queries(execution_trace_id, &["avatar", "text:com.twitter"]);

            let mut trace = resolution_execution_trace(
                execution_trace_id,
                &request_key,
                &["avatar", "text:com.twitter"],
                persisted_verified_queries.clone(),
            );
            trace.request_metadata = json!({
                "surface": "alice.eth",
                "record_keys": ["avatar", "text:com.twitter"],
                "entrypoint": "universal_resolver",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "alias": {
                    "final_target": alias_target.clone(),
                    "hops": [alias_target.clone()],
                }
            });
            upsert_execution_trace(&database.pool, &trace).await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed alias resolution request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolutionResponse = read_json(response).await?;
            let declared_state = payload
                .declared_state
                .as_ref()
                .expect("declared_state must be present");

            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(
                declared_state.get("topology"),
                Some(&json!({
                    "status": "unsupported",
                    "unsupported_reason": "declared resolution topology is not yet projected",
                }))
            );
            assert!(
                declared_state
                    .get("record_inventory")
                    .and_then(|value| value.get("record_version_boundary"))
                    .is_some(),
                "record inventory should still load through the alias-only readback lane"
            );
            assert_eq!(
                payload.verified_state,
                Some(json!({
                    "verified_queries": resolution_alias_only_verified_queries(
                        execution_trace_id,
                        &["avatar", "text:com.twitter"],
                    ),
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_reads_persisted_alias_only_avatar_answer_and_reuses_resolution_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
            let alias_target = json!({
                "logical_name_id": "ens:profile.alice.eth",
                "namespace": "ens",
                "normalized_name": "profile.alice.eth",
                "canonical_display_name": "Profile.alice.eth",
                "namehash": "namehash:profile.alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "resolver_alias_path",
            });

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("alias execution explain requires an exact-name current row")?;
            name_row.binding_kind = Some(SurfaceBindingKind::ResolverAliasPath);
            database.insert_name_current_row(name_row.clone()).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let alias_records =
                parse_resolution_record_keys(Some("text:com.twitter"), ResolutionMode::Verified)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &alias_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries =
                resolution_alias_only_verified_queries(execution_trace_id, &["avatar", "text:com.twitter"]);

            let mut trace = resolution_execution_trace(
                execution_trace_id,
                &request_key,
                &["avatar", "text:com.twitter"],
                persisted_verified_queries.clone(),
            );
            trace.request_metadata = json!({
                "surface": "alice.eth",
                "record_keys": ["avatar", "text:com.twitter"],
                "entrypoint": "universal_resolver",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "alias": {
                    "final_target": alias_target.clone(),
                    "hops": [alias_target.clone()],
                }
            });
            upsert_execution_trace(&database.pool, &trace).await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("alias execution explain request failed")?;
            let resolution_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("alias verified resolution request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(resolution_response.status(), StatusCode::OK);

            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
            let mut expected_execution = resolution_execution_summary(execution_trace_id, resource_id);
            expected_execution["alias"] = json!({
                "final_target": alias_target.clone(),
                "hops": [alias_target.clone()],
            });

            assert_eq!(explain_payload.data, resolution_payload.data);
            assert_eq!(explain_payload.coverage, resolution_payload.coverage);
            assert_eq!(explain_payload.provenance, resolution_payload.provenance);
            assert_eq!(
                explain_payload.chain_positions,
                resolution_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, resolution_payload.consistency);
            assert_eq!(
                explain_payload.last_updated,
                resolution_payload.last_updated
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                resolution_payload.verified_state,
                Some(json!({
                    "verified_queries": persisted_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": expected_execution,
                    "verified_queries": persisted_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_persisted_wildcard_derived_answer_on_mixed_route_and_reuses_execution_explain_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let wildcard_source_logical_name_id = "ens:eth";
            let wildcard_source_resource_id = Uuid::from_u128(0x4400);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("wildcard-derived mixed resolution requires an exact-name current row")?;
            let projected_topology = resolution_wildcard_projected_topology(
                logical_name_id,
                resource_id,
                wildcard_source_logical_name_id,
                wildcard_source_resource_id,
            );
            name_row.binding_kind = Some(SurfaceBindingKind::ObservedWildcardPath);
            name_row.declared_summary = json!({
                "topology": projected_topology.clone(),
            });
            database.insert_name_current_row(name_row.clone()).await?;

            let records = parse_resolution_record_keys(Some("addr:60"), ResolutionMode::Verified)
                .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(&name_row, &records, None)?;
            let request_key = cache_key.request_key.clone();
            let expected_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["addr:60"]);

            let mut trace = resolution_execution_trace(
                execution_trace_id,
                &request_key,
                &["addr:60"],
                expected_verified_queries.clone(),
            );
            trace.request_metadata = json!({
                "surface": "alice.eth",
                "record_keys": ["addr:60"],
                "entrypoint": "universal_resolver",
                "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                "binding_kind": "observed_wildcard_path",
                "wildcard": {
                    "source": resolution_wildcard_source(
                        wildcard_source_logical_name_id,
                        wildcard_source_resource_id,
                    ),
                    "matched_labels": ["alice"],
                }
            });
            trace.steps.push(ExecutionTraceStep {
                step_index: 2,
                step_kind: "call_wildcard_resolver".to_owned(),
                input_digest: Some("sha256:wildcard-input".to_owned()),
                output_digest: Some("sha256:wildcard-output".to_owned()),
                latency_ms: Some(19),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical",
                    }
                }),
                step_payload: json!({
                    "name": "alice.eth",
                    "wildcard": {
                        "source": resolution_wildcard_source(
                            wildcard_source_logical_name_id,
                            wildcard_source_resource_id,
                        ),
                        "matched_labels": ["alice"],
                    }
                }),
            });

            upsert_execution_trace(&database.pool, &trace).await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key,
                    expected_verified_queries.clone(),
                ),
            )
            .await?;

            let mixed_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolutions/ens/alice.eth?mode=both&records=addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("wildcard-derived mixed resolution request failed")?;
            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("wildcard-derived resolution execution explain request failed")?;

            assert_eq!(mixed_response.status(), StatusCode::OK);
            assert_eq!(explain_response.status(), StatusCode::OK);

            let mixed_payload: ResolutionResponse = read_json(mixed_response).await?;
            let explain_payload: ResolutionResponse = read_json(explain_response).await?;
            let expected_declared_state = json!({
                "topology": projected_topology.clone(),
                "record_inventory": {
                    "status": "unsupported",
                    "unsupported_reason": "declared resolution record inventory is not yet projected",
                },
                "record_cache": {
                    "status": "unsupported",
                    "unsupported_reason": "declared resolution record cache is not yet projected",
                },
            });
            let expected_execution = resolution_wildcard_execution_summary(
                execution_trace_id,
                wildcard_source_logical_name_id,
                wildcard_source_resource_id,
            );

            assert_eq!(mixed_payload.data, explain_payload.data);
            assert_eq!(mixed_payload.coverage, explain_payload.coverage);
            assert_eq!(
                mixed_payload.chain_positions,
                explain_payload.chain_positions
            );
            assert_eq!(mixed_payload.consistency, explain_payload.consistency);
            assert_eq!(mixed_payload.last_updated, explain_payload.last_updated);
            assert_eq!(
                mixed_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(
                mixed_payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(
                mixed_payload.verified_state,
                Some(json!({
                    "verified_queries": expected_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(execution_trace_id.to_string()))
            );
            assert_eq!(explain_payload.declared_state, None);
            assert_eq!(
                explain_payload.verified_state,
                Some(json!({
                    "execution": expected_execution,
                    "verified_queries": expected_verified_queries,
                }))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_returns_selector_local_unsupported_for_non_alias_ancestor_selected_requests()
        -> Result<()> {
            run_resolution_negative_verified_path_case(
                UnsupportedEnsVerifiedResolutionPathCase::NonAliasAncestorSelected,
            )
            .await
        }

        #[tokio::test]
        async fn resolution_contract_returns_selector_local_unsupported_for_transport_assisted_requests()
        -> Result<()> {
            run_resolution_negative_verified_path_case(
                UnsupportedEnsVerifiedResolutionPathCase::TransportAssisted,
            )
            .await
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_returns_not_found_for_non_alias_ancestor_selected_requests()
        -> Result<()> {
            run_resolution_execution_explain_negative_verified_path_case(
                UnsupportedEnsVerifiedResolutionPathCase::NonAliasAncestorSelected,
            )
            .await
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_returns_not_found_for_transport_assisted_requests()
        -> Result<()> {
            run_resolution_execution_explain_negative_verified_path_case(
                UnsupportedEnsVerifiedResolutionPathCase::TransportAssisted,
            )
            .await
        }

        #[tokio::test]
        async fn resolution_execution_explain_contract_returns_not_found_for_selector_set_cache_miss()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000022);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution explain requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let persisted_records = parse_resolution_record_keys(Some("addr:60"), ResolutionMode::Verified)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &persisted_records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries =
                resolution_execution_verified_queries(execution_trace_id, &["addr:60"]);

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(execution_trace_id, cache_key, persisted_verified_queries),
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed")?;

            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "not_found");
            assert_eq!(
                payload.error.message,
                "persisted resolution execution explain was not found for name alice.eth in namespace ens"
            );
            assert!(payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolution_execution_invalidation_contract_evicts_persisted_answers_from_mixed_and_explain_routes()
        -> Result<()> {
            for invalidation in [
                PersistedResolutionInvalidation::Manifest,
                PersistedResolutionInvalidation::Topology,
                PersistedResolutionInvalidation::Record,
            ] {
                run_resolution_execution_invalidation_case(invalidation)
                    .await
                    .with_context(|| {
                        format!(
                            "persisted verified resolution invalidation failed for {}",
                            invalidation.label()
                        )
                    })?;
            }

            Ok(())
        }

        #[tokio::test]
        async fn resolution_contract_reads_ensv2_record_inventory_and_declared_cache_statuses() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x9200);
            let token_lineage_id = Uuid::from_u128(0x9201);
            let surface_binding_id = Uuid::from_u128(0x9202);
            let resolver_address = "0x0000000000000000000000000000000000000abc";
            let namehash = "namehash:alice.eth";

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            seed_ens_v2_event_fixture_inputs(
                &database.pool,
                &[
                    ens_v2_record_version_changed_event(
                        "conformance:ensv2:alice:record-version",
                        logical_name_id,
                        resource_id,
                        resolver_address,
                        namehash,
                        "2",
                        15,
                        121,
                        0,
                    ),
                    ens_v2_record_changed_event(
                        "conformance:ensv2:alice:addr60",
                        logical_name_id,
                        resource_id,
                        resolver_address,
                        namehash,
                        "addr",
                        Some("60"),
                        16,
                        122,
                        0,
                    ),
                    ens_v2_record_changed_event(
                        "conformance:ensv2:alice:text",
                        logical_name_id,
                        resource_id,
                        resolver_address,
                        namehash,
                        "text",
                        None,
                        16,
                        122,
                        1,
                    ),
                    ens_v2_record_changed_event(
                        "conformance:ensv2:alice:pubkey",
                        logical_name_id,
                        resource_id,
                        resolver_address,
                        namehash,
                        "pubkey",
                        None,
                        17,
                        123,
                        0,
                    ),
                ],
            )
            .await?;
            rebuild_record_inventory_current(&database, resource_id).await?;
            let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("ENSv2 declared resolution inventory test requires name_current row")?;
            name_row.chain_positions["ethereum"] = json!({
                "chain_id": "ethereum-mainnet",
                "block_number": 121,
                "block_hash": "0xensv2block79",
                "timestamp": "2024-05-31T21:15:21Z",
            });
            database.insert_name_current_row(name_row).await?;

            let response = app_router(database.app_state())
                        .oneshot(
                            Request::builder()
                                .uri(
                                    "/v1/resolutions/ens/alice.eth?mode=declared&records=addr:60,text,contenthash,pubkey",
                                )
                                .body(Body::empty())
                                .expect("request must build"),
                        )
                        .await
                        .context("ENSv2 declared resolution inventory request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolutionResponse = read_json(response).await?;
            let declared_state = payload
                .declared_state
                .as_ref()
                .context("ENSv2 declared resolution must include declared_state")?;
            let topology_record_boundary = declared_state
                .get("topology")
                .and_then(|value| value.get("version_boundaries"))
                .and_then(|value| value.get("record_version_boundary"))
                .context("ENSv2 topology must expose record_version_boundary")?;
            let record_inventory = declared_state
                .get("record_inventory")
                .context("ENSv2 declared resolution must include record_inventory")?;
            let record_cache = declared_state
                .get("record_cache")
                .context("ENSv2 declared resolution must include record_cache")?;

            assert_eq!(
                record_inventory.get("record_version_boundary"),
                Some(topology_record_boundary)
            );
            assert_eq!(
                record_cache.get("record_version_boundary"),
                Some(topology_record_boundary)
            );
            assert_eq!(
                record_inventory
                    .get("enumeration_basis")
                    .and_then(|value| value.get("globally_enumerable")),
                Some(&json!(false))
            );
            assert_eq!(
                record_inventory
                    .get("selectors")
                    .and_then(Value::as_array)
                    .expect("ENSv2 record_inventory selectors must be an array")
                    .iter()
                    .map(record_selector_identity_tuple)
                    .collect::<Vec<_>>(),
                vec![
                    (
                        "addr:60".to_owned(),
                        "addr".to_owned(),
                        Some("60".to_owned()),
                    ),
                    ("text".to_owned(), "text".to_owned(), None),
                ]
            );
            assert_eq!(
                record_inventory
                    .get("unsupported_families")
                    .and_then(Value::as_array)
                    .expect("ENSv2 record_inventory unsupported_families must be an array"),
                &vec![json!({
                    "record_family": "pubkey",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                })]
            );
            assert_eq!(
                record_cache.get("entries"),
                Some(&json!([
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events",
                    },
                    {
                        "record_key": "text",
                        "record_family": "text",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events",
                    },
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    },
                    {
                        "record_key": "pubkey",
                        "record_family": "pubkey",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                    }
                ]))
            );
            assert_eq!(payload.verified_state, None);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolver_overview_contract_returns_declared_state_with_shared_projection_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let chain_id = "ethereum-mainnet";
            let resolver_address = "0x0000000000000000000000000000000000000aaa";

            bigname_storage::upsert_resolver_current_rows(
                &database.pool,
                &[resolver_current_row(chain_id, resolver_address)],
            )
            .await
            .context("failed to upsert resolver_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolver overview request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolverResponse = read_json(response).await?;
            assert_eq!(
                payload.data,
                json!({
                    "chain_id": chain_id,
                    "resolver_address": resolver_address,
                })
            );
            assert_eq!(
                payload.declared_state,
                json!({
                    "bindings": {
                        "status": "supported",
                        "count": 2,
                        "items": [
                            {
                                "logical_name_id": "ens:alice.eth",
                                "canonical_display_name": "Alice.eth",
                                "normalized_name": "alice.eth",
                                "namehash": "namehash:alice.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b100",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                                "binding_kind": "declared_registry_path",
                            },
                            {
                                "logical_name_id": "ens:beta.eth",
                                "canonical_display_name": "Beta.eth",
                                "normalized_name": "beta.eth",
                                "namehash": "namehash:beta.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b102",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                                "binding_kind": "resolver_alias_path",
                            }
                        ],
                    },
                    "aliases": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "logical_name_id": "ens:beta.eth",
                            "canonical_display_name": "Beta.eth",
                            "normalized_name": "beta.eth",
                            "namehash": "namehash:beta.eth",
                            "resource_id": "00000000-0000-0000-0000-00000000b102",
                            "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                            "binding_kind": "resolver_alias_path",
                        }],
                    },
                    "permissions": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "resource_id": "00000000-0000-0000-0000-00000000b100",
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "effective_powers": ["set_resolver", "set_records"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "resolver-permission-1",
                            },
                            "revocation_source": null,
                        }],
                    },
                    "role_holders": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "resource_count": 1,
                            "permission_row_count": 1,
                            "effective_powers": ["set_records", "set_resolver"],
                            "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                        }],
                    },
                    "event_summary": {
                        "status": "supported",
                        "count": 2,
                        "by_kind": {
                            "PermissionChanged": 1,
                            "ResolverChanged": 1,
                        },
                    },
                })
            );
            assert_eq!(payload.verified_state, None);
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": ["101", "202"],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "chain_id": chain_id,
                        "block_number": 202,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 7,
                        "source_family": "ens_v2_registry_l1",
                        "chain": chain_id,
                        "deployment_epoch": "ens_v2",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "resolver_current_rebuild",
                })
            );
            assert_eq!(
                payload.coverage,
                json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
                    "enumeration_basis": "resolver_target",
                    "unsupported_reason": null,
                })
            );
            assert_eq!(
                payload.chain_positions,
                json!({
                    "ethereum": {
                        "chain_id": chain_id,
                        "block_number": 202,
                        "block_hash": "0xresolverc8",
                        "timestamp": "2026-04-17T00:00:22Z",
                    }
                })
            );
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.last_updated, "2025-06-01T17:50:02Z");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolver_overview_contract_reads_basenames_truth_from_resolver_and_permissions_current()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0xa3b0);
            let token_lineage_id = Uuid::from_u128(0xa3b1);
            let surface_binding_id = Uuid::from_u128(0xa3b2);
            let resolver_address = "0x0000000000000000000000000000000000000abc";
            let subject = BasenamesControlVectorScenario::ManagementOnly.current_effective_controller();

            seed_basenames_control_vector_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
                BasenamesControlVectorScenario::ManagementOnly,
            )
            .await?;
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        "0xbase-permission-1",
                        None,
                        106,
                        1_717_181_706,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xbase-permission-2",
                        None,
                        107,
                        1_717_181_707,
                    ),
                ],
            )
            .await?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:basenames:resolver-permission-1".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(106),
                        block_hash: Some("0xbase-permission-1".to_owned()),
                        transaction_hash: Some("0xtxbasepermission1".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resolver-permission-1"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resolver",
                                "chain_id": "base-mainnet",
                                "resolver_address": "0x0000000000000000000000000000000000000AbC",
                            },
                            "effective_powers": ["resolver_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:basenames:resolver-permission-1",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:basenames:resolver-permission-2".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xbase-permission-2".to_owned()),
                        transaction_hash: Some("0xtxbasepermission2".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resolver-permission-2"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resolver",
                                "chain_id": "base-mainnet",
                                "resolver_address": resolver_address,
                            },
                            "effective_powers": ["resolver_control", "resource_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:basenames:resolver-permission-2",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                ],
            )
            .await?;
            rebuild_resolver_current(&database, Some("base-mainnet"), Some(resolver_address)).await?;

            let raw_only_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/base-mainnet/0x0000000000000000000000000000000000000ABC")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("Basenames raw-only resolver overview contract request failed")?;

            assert_eq!(raw_only_response.status(), StatusCode::OK);

            let raw_only_payload: ResolverResponse = read_json(raw_only_response).await?;
            assert_eq!(
                raw_only_payload.declared_state["bindings"]["count"],
                json!(1)
            );
            assert_eq!(
                raw_only_payload.declared_state["permissions"],
                json!({
                    "status": "supported",
                    "count": 0,
                    "items": [],
                })
            );
            assert_eq!(
                raw_only_payload.declared_state["role_holders"],
                json!({
                    "status": "supported",
                    "count": 0,
                    "items": [],
                })
            );
            assert_eq!(
                raw_only_payload.declared_state["event_summary"],
                json!({
                    "status": "supported",
                    "count": 1,
                    "by_kind": {
                        "ResolverChanged": 1,
                    },
                })
            );

            rebuild_permissions_current(&database, Some(resource_id)).await?;
            rebuild_resolver_current(&database, Some("base-mainnet"), Some(resolver_address)).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/base-mainnet/0x0000000000000000000000000000000000000ABC")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("Basenames resolver overview contract request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolverResponse = read_json(response).await?;
            assert_eq!(
                payload.data,
                json!({
                    "chain_id": "base-mainnet",
                    "resolver_address": resolver_address,
                })
            );
            assert_eq!(payload.declared_state["bindings"]["count"], json!(1));
            assert_eq!(
                payload.declared_state["aliases"],
                json!({
                "status": "supported",
                "count": 0,
                "items": [],
                })
            );
            assert_eq!(
                payload.declared_state["permissions"]["items"][0],
                json!({
                    "resource_id": resource_id.to_string(),
                    "subject": subject,
                    "effective_powers": ["resolver_control", "resource_control"],
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": "conformance:basenames:resolver-permission-2",
                    },
                    "revocation_source": null,
                })
            );
            assert_eq!(
                payload.declared_state["event_summary"],
                json!({
                    "status": "supported",
                    "count": 3,
                    "by_kind": {
                        "PermissionChanged": 2,
                        "ResolverChanged": 1,
                    },
                })
            );
            assert_eq!(payload.verified_state, None);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resolver_overview_contract_reads_ensv2_summary_without_expanding_permission_ledger()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x9300);
            let other_resource_id = Uuid::from_u128(0x9303);
            let token_lineage_id = Uuid::from_u128(0x9301);
            let surface_binding_id = Uuid::from_u128(0x9302);
            let resolver_address = "0x0000000000000000000000000000000000000aaa";
            let subject = "0x0000000000000000000000000000000000000abc";
            let other_subject = "0x0000000000000000000000000000000000000def";

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[ens_v2_resource(
                    other_resource_id,
                    132,
                    "ensv2_resolver_permission_other_resource",
                )],
            )
            .await
            .context("failed to upsert second ENSv2 resolver permission resource")?;
            seed_ens_v2_event_fixture_inputs(
                &database.pool,
                &[
                    ens_v2_resolver_event(
                        "conformance:ensv2:alice:resolver-overview",
                        logical_name_id,
                        resource_id,
                        resolver_address,
                        "ResolverChanged",
                        10,
                        131,
                        0,
                        json!({}),
                        json!({
                            "resolver": resolver_address,
                            "namehash": "namehash:alice.eth",
                        }),
                    ),
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:alice:resolver-permission",
                        logical_name_id,
                        resource_id,
                        subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: resolver_address.to_owned(),
                        },
                        &["set_records", "set_resolver"],
                        11,
                        132,
                        0,
                    ),
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:alice:resolver-permission-other-resource",
                        logical_name_id,
                        other_resource_id,
                        other_subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: resolver_address.to_owned(),
                        },
                        &["set_records"],
                        12,
                        133,
                        0,
                    ),
                ],
            )
            .await?;
            rebuild_resolver_current(&database, Some("ethereum-mainnet"), Some(resolver_address)).await?;

            let raw_only_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 raw-only resolver overview request failed")?;

            assert_eq!(raw_only_response.status(), StatusCode::OK);

            let raw_only_payload: ResolverResponse = read_json(raw_only_response).await?;
            assert_eq!(
                raw_only_payload.declared_state["bindings"]["count"],
                json!(1)
            );
            assert_eq!(
                raw_only_payload.declared_state["permissions"],
                json!({
                    "status": "supported",
                    "count": 0,
                    "items": [],
                })
            );
            assert_eq!(
                raw_only_payload.declared_state["role_holders"],
                json!({
                    "status": "supported",
                    "count": 0,
                    "items": [],
                })
            );
            assert_eq!(
                raw_only_payload.declared_state["event_summary"],
                json!({
                    "status": "supported",
                    "count": 1,
                    "by_kind": {
                        "ResolverChanged": 1,
                    },
                })
            );

            rebuild_permissions_current(&database, Some(resource_id)).await?;
            rebuild_permissions_current(&database, Some(other_resource_id)).await?;

            let first_permission_rows = bigname_storage::load_permissions_current(
                &database.pool,
                resource_id,
                None,
                Some(&PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: resolver_address.to_owned(),
                }),
            )
            .await?;
            let second_permission_rows = bigname_storage::load_permissions_current(
                &database.pool,
                other_resource_id,
                None,
                Some(&PermissionScope::Resolver {
                    chain_id: "ethereum-mainnet".to_owned(),
                    resolver_address: resolver_address.to_owned(),
                }),
            )
            .await?;
            assert_eq!(first_permission_rows.len(), 1);
            assert_eq!(second_permission_rows.len(), 1);
            assert_ne!(
                first_permission_rows[0].resource_id,
                second_permission_rows[0].resource_id
            );

            rebuild_resolver_current(&database, Some("ethereum-mainnet"), Some(resolver_address)).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/resolvers/ethereum-mainnet/0x0000000000000000000000000000000000000AAA")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 resolver overview request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResolverResponse = read_json(response).await?;
            assert_eq!(
                payload.data,
                json!({
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": resolver_address,
                })
            );
            assert_eq!(payload.declared_state["bindings"]["count"], json!(1));
            assert_eq!(
                payload.declared_state["bindings"]["items"][0]["logical_name_id"],
                json!(logical_name_id)
            );
            assert_eq!(payload.declared_state["aliases"]["count"], json!(0));
            assert_eq!(payload.declared_state["permissions"]["count"], json!(2));
            assert_eq!(
                payload.declared_state["permissions"]["items"][0],
                json!({
                    "resource_id": resource_id.to_string(),
                    "subject": subject,
                    "effective_powers": ["set_records", "set_resolver"],
                    "grant_source": {
                        "kind": "raw_log",
                        "source_event": "EACRolesChanged",
                        "resource_id": resource_id.to_string(),
                        "changed_powers": ["set_records", "set_resolver"],
                    },
                    "revocation_source": null,
                })
            );
            assert_eq!(
                payload.declared_state["permissions"]["items"][1],
                json!({
                    "resource_id": other_resource_id.to_string(),
                    "subject": other_subject,
                    "effective_powers": ["set_records"],
                    "grant_source": {
                        "kind": "raw_log",
                        "source_event": "EACRolesChanged",
                        "resource_id": other_resource_id.to_string(),
                        "changed_powers": ["set_records"],
                    },
                    "revocation_source": null,
                })
            );
            assert_eq!(
                payload.declared_state["permissions"]["items"][0]
                    .as_object()
                    .expect("resolver permission summary item must be an object")
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([
                    "effective_powers".to_owned(),
                    "grant_source".to_owned(),
                    "resource_id".to_owned(),
                    "revocation_source".to_owned(),
                    "subject".to_owned(),
                ])
            );
            assert_eq!(
                payload.declared_state["role_holders"]["items"][0],
                json!({
                    "subject": subject,
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records", "set_resolver"],
                    "resource_ids": [resource_id.to_string()],
                })
            );
            assert_eq!(
                payload.declared_state["role_holders"]["items"][1],
                json!({
                    "subject": other_subject,
                    "resource_count": 1,
                    "permission_row_count": 1,
                    "effective_powers": ["set_records"],
                    "resource_ids": [other_resource_id.to_string()],
                })
            );
            assert_eq!(
                payload.declared_state["event_summary"],
                json!({
                    "status": "supported",
                    "count": 3,
                    "by_kind": {
                        "PermissionChanged": 2,
                        "ResolverChanged": 1,
                    },
                })
            );
            assert_eq!(
                payload.coverage.get("enumeration_basis"),
                Some(&json!("resolver_overview"))
            );
            assert_eq!(
                payload.coverage.get("source_classes_considered"),
                Some(&json!(["ens_v2_resolver_l1"]))
            );
            assert_eq!(payload.verified_state, None);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_returns_rows_with_shared_collection_envelope() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let resource_id = Uuid::from_u128(0xa300);
            let filtered_subject = "0x0000000000000000000000000000000000000abc";
            let other_subject = "0x0000000000000000000000000000000000000def";

            bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)])
                .await
                .context("failed to upsert resource for permissions conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(
                        resource_id,
                        filtered_subject,
                        PermissionScope::Resource,
                        7,
                        41,
                    ),
                    permission_current_row(
                        resource_id,
                        filtered_subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000aaa".to_owned(),
                        },
                        8,
                        42,
                    ),
                    permission_current_row(resource_id, other_subject, PermissionScope::Registry, 9, 43),
                ],
            )
            .await
            .context("failed to upsert permissions_current rows for conformance")?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResourcePermissionsResponse = read_json(response).await?;
            assert_eq!(
                permission_subjects(&payload),
                vec![filtered_subject, filtered_subject, other_subject]
            );
            assert!(payload.verified_state.is_none());
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.page_size, 3);
            assert_eq!(payload.page.sort, "subject_scope_asc");
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.coverage.exhaustiveness, "authoritative");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["permissions_current".to_owned()]
            );
            assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
            assert_eq!(payload.coverage.unsupported_reason, None);
            assert_eq!(
                payload
                    .provenance
                    .get("derivation_kind")
                    .and_then(Value::as_str),
                Some("permissions_current_rebuild")
            );

            let resource_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resource")
                })
                .expect("resource row");
            assert_eq!(
                resource_row.get("resource_id"),
                Some(&Value::String(resource_id.to_string()))
            );
            assert_eq!(
                resource_row.get("scope"),
                Some(&json!({
                    "kind": "resource",
                    "detail": {},
                }))
            );
            assert_eq!(
                resource_row.get("effective_powers"),
                Some(&json!(["set_resolver", "set_records"]))
            );
            assert_eq!(resource_row.get("revocation_source"), Some(&Value::Null));

            let resolver_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resolver")
                })
                .expect("resolver row");
            assert_eq!(
                resolver_row.get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000aaa",
                    },
                }))
            );

            let first_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions first page request failed")?;
            assert_eq!(first_page_response.status(), StatusCode::OK);
            let first_page_payload: ResourcePermissionsResponse = read_json(first_page_response).await?;
            let cursor = first_page_payload
                .page
                .next_cursor
                .clone()
                .expect("resource permissions first page must include next_cursor");

            let second_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions second page request failed")?;
            assert_eq!(second_page_response.status(), StatusCode::OK);
            let second_page_payload: ResourcePermissionsResponse = read_json(second_page_response).await?;

            let replay_page_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?page_size=1&cursor={cursor}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions replay page request failed")?;
            assert_eq!(replay_page_response.status(), StatusCode::OK);
            let replay_page_payload: ResourcePermissionsResponse = read_json(replay_page_response).await?;

            assert_replay_stable_pagination(
                &payload.data,
                &payload.page,
                &first_page_payload.data,
                &first_page_payload.page,
                &second_page_payload.data,
                &second_page_payload.page,
                &replay_page_payload.data,
                &replay_page_payload.page,
                "subject_scope_asc",
                3,
                1,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_reads_basenames_permission_changed_rows_only() -> Result<()>
        {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:management-only.base.eth";
            let resource_id = Uuid::from_u128(0xa3c0);
            let token_lineage_id = Uuid::from_u128(0xa3c1);
            let surface_binding_id = Uuid::from_u128(0xa3c2);
            let subject = BasenamesControlVectorScenario::ManagementOnly.current_effective_controller();

            seed_basenames_control_vector_rebuild_inputs(
                &database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
                BasenamesControlVectorScenario::ManagementOnly,
            )
            .await?;
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        "0xbase-permission-3",
                        None,
                        106,
                        1_717_181_706,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xbase-permission-4",
                        None,
                        107,
                        1_717_181_707,
                    ),
                ],
            )
            .await?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:basenames:resource-permission".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(106),
                        block_hash: Some("0xbase-permission-3".to_owned()),
                        transaction_hash: Some("0xtxbasepermission3".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resource-permission"}),
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
                                "event_identity": "conformance:basenames:resource-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                    NormalizedEvent {
                        event_identity:
                            "conformance:basenames:resolver-permission-role-summary".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xbase-permission-4".to_owned()),
                        transaction_hash: Some("0xtxbasepermission4".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resolver-permission-role-summary"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resolver",
                                "chain_id": "base-mainnet",
                                "resolver_address": "0x0000000000000000000000000000000000000abc",
                            },
                            "effective_powers": ["resolver_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:basenames:resolver-permission-role-summary",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                ],
            )
            .await?;
            rebuild_permissions_current(&database, Some(resource_id)).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("Basenames resource permissions contract request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResourcePermissionsResponse = read_json(response).await?;
            assert_eq!(permission_subjects(&payload), vec![subject, subject]);
            assert_eq!(payload.page.sort, "subject_scope_asc");
            assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
            assert_eq!(payload.coverage.unsupported_reason, None);
            let resource_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resource")
                })
                .expect("resource row");
            assert_eq!(
                resource_row.get("effective_powers"),
                Some(&json!(["resource_control"]))
            );
            let resolver_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resolver")
                })
                .expect("resolver row");
            assert_eq!(
                resolver_row.get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "base-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000abc",
                    },
                }))
            );
            assert_eq!(
                resolver_row.get("effective_powers"),
                Some(&json!(["resolver_control"]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_reads_ensv2_resource_and_resolver_scopes() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x9400);
            let subject = "0x0000000000000000000000000000000000000abc";
            let resolver_address = "0x0000000000000000000000000000000000000aaa";

            bigname_storage::upsert_resources(
                &database.pool,
                &[ens_v2_resource(
                    resource_id,
                    141,
                    "ensv2_permissions_resource",
                )],
            )
            .await
            .context("failed to upsert ENSv2 permission resource")?;
            seed_ens_v2_event_fixture_inputs(
                &database.pool,
                &[
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:alice:resource-permission",
                        logical_name_id,
                        resource_id,
                        subject,
                        PermissionScope::Resource,
                        &["resource_control"],
                        12,
                        142,
                        0,
                    ),
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:alice:resolver-permission-row",
                        logical_name_id,
                        resource_id,
                        subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: resolver_address.to_owned(),
                        },
                        &["resolver_control"],
                        13,
                        143,
                        0,
                    ),
                    ens_v2_permission_changed_event(
                        "conformance:ensv2:alice:revoked-resolver-permission",
                        logical_name_id,
                        resource_id,
                        "0x0000000000000000000000000000000000000def",
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                        },
                        &[],
                        14,
                        144,
                        0,
                    ),
                ],
            )
            .await?;
            rebuild_permissions_current(&database, Some(resource_id)).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/resources/{resource_id}/permissions"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("ENSv2 resource permissions request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: ResourcePermissionsResponse = read_json(response).await?;
            assert_eq!(payload.data.len(), 2);
            assert_eq!(permission_subjects(&payload), vec![subject, subject]);
            assert_eq!(payload.page.sort, "subject_scope_asc");
            assert_eq!(payload.coverage.enumeration_basis, "resource_permissions");
            assert_eq!(
                payload.coverage.source_classes_considered,
                vec!["ens_v2_resolver_l1".to_owned()]
            );
            assert_eq!(payload.verified_state, None);
            assert_eq!(payload.declared_state, json!({}));

            let resource_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resource")
                })
                .expect("ENSv2 resource-scoped permission row must be present");
            assert_eq!(
                resource_row.get("scope"),
                Some(&json!({
                    "kind": "resource",
                    "detail": {},
                }))
            );
            assert_eq!(
                resource_row.get("effective_powers"),
                Some(&json!(["resource_control"]))
            );

            let resolver_row = payload
                .data
                .iter()
                .find(|row| {
                    row.get("scope")
                        .and_then(|value| value.get("kind"))
                        .and_then(Value::as_str)
                        == Some("resolver")
                })
                .expect("ENSv2 resolver-scoped permission row must be present");
            assert_eq!(
                resolver_row.get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": resolver_address,
                    },
                }))
            );
            assert_eq!(
                resolver_row.get("effective_powers"),
                Some(&json!(["resolver_control"]))
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_permissions_contract_honors_subject_and_scope_filters() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let resource_id = Uuid::from_u128(0xa301);
            let shared_subject = "0x0000000000000000000000000000000000000abc";

            bigname_storage::upsert_resources(&database.pool, &[resource(resource_id)])
                .await
                .context("failed to upsert resource for permissions filter conformance")?;
            bigname_storage::upsert_permissions_current_rows(
                &database.pool,
                &[
                    permission_current_row(
                        resource_id,
                        shared_subject,
                        PermissionScope::Resource,
                        7,
                        51,
                    ),
                    permission_current_row(
                        resource_id,
                        shared_subject,
                        PermissionScope::Resolver {
                            chain_id: "ethereum-mainnet".to_owned(),
                            resolver_address: "0x0000000000000000000000000000000000000bbb".to_owned(),
                        },
                        8,
                        52,
                    ),
                    permission_current_row(
                        resource_id,
                        "0x0000000000000000000000000000000000000def",
                        PermissionScope::Resource,
                        9,
                        53,
                    ),
                ],
            )
            .await
            .context("failed to upsert permissions_current filter rows for conformance")?;

            let subject_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?subject={shared_subject}"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions subject filter request failed")?;
            assert_eq!(subject_response.status(), StatusCode::OK);

            let subject_payload: ResourcePermissionsResponse = read_json(subject_response).await?;
            assert_eq!(
                permission_subjects(&subject_payload),
                vec![shared_subject, shared_subject]
            );

            let scope_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/resources/{resource_id}/permissions?scope=resolver:ethereum-mainnet:0x0000000000000000000000000000000000000bbb"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource permissions scope filter request failed")?;
            assert_eq!(scope_response.status(), StatusCode::OK);

            let scope_payload: ResourcePermissionsResponse = read_json(scope_response).await?;
            assert_eq!(scope_payload.data.len(), 1);
            assert_eq!(
                scope_payload.data[0].get("scope"),
                Some(&json!({
                    "kind": "resolver",
                    "detail": {
                        "chain_id": "ethereum-mainnet",
                        "resolver_address": "0x0000000000000000000000000000000000000bbb",
                    },
                }))
            );

            database.cleanup().await?;
            Ok(())
        }
