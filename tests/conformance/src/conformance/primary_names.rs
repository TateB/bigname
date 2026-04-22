        #[tokio::test]
        async fn primary_names_contract_reads_status_shaped_declared_results_by_mode_for_tuple_present()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            database
                .seed_primary_name_reverse_changed(address, "60")
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", "60")
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name bootstrap request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name bootstrap request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name bootstrap request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_basenames_declared_claimed_name_for_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bcd";
            let expected_data = json!({
                "address": address,
                "namespace": "basenames",
                "coin_type": "60",
            });

            database
                .seed_basenames_primary_name_claim_observation(address, "60", "Alice.base.eth")
                .await?;
            database
                .rebuild_primary_names_current(address, "basenames", "60")
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared basenames primary-name claimed-name request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed basenames primary-name claimed-name request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "success",
                        "name": "alice.base.eth",
                        "provenance": {
                            "source_family": "basenames_base_primary",
                            "contract_role": "reverse_registrar",
                            "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                            "emitting_address": "0x00000000000000000000000000000000000000ad",
                        },
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(
                both_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            for payload in [&declared_payload, &both_payload] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_declared_claimed_name_for_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let target_expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });
            let sibling_expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "61",
            });

            upsert_primary_name_current_snapshots(
                &database.pool,
                &[
                    PrimaryNameCurrentSnapshot {
                        row: PrimaryNameCurrentRow {
                            address: address.to_owned(),
                            namespace: "ens".to_owned(),
                            coin_type: "60".to_owned(),
                            claim_status: PrimaryNameClaimStatus::Success,
                            raw_claim_name: None,
                            claim_provenance: json!({
                                "source_family": "target_reverse",
                                "contract_role": "reverse_registrar",
                                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                                "emitting_address": "0x00000000000000000000000000000000000000ad",
                                "execution_trace_id": "must-be-omitted",
                                "verified_primary_name_lookup": {
                                    "address": address,
                                    "namespace": "ens",
                                    "coin_type": "60",
                                },
                                "verified_primary_name_invalidation": {
                                    "claim_status": "success",
                                    "primary_claim_source": {
                                        "seed": "ignored",
                                    },
                                },
                            }),
                        },
                        normalized_claim_name: Some("alice.eth".to_owned()),
                    },
                    PrimaryNameCurrentSnapshot {
                        row: PrimaryNameCurrentRow {
                            address: address.to_owned(),
                            namespace: "ens".to_owned(),
                            coin_type: "61".to_owned(),
                            claim_status: PrimaryNameClaimStatus::Success,
                            raw_claim_name: None,
                            claim_provenance: json!({
                                "source_family": "sibling_reverse",
                            }),
                        },
                        normalized_claim_name: Some("beta.eth".to_owned()),
                    },
                ],
            )
            .await?;

            let target_declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name claimed-name request failed")?;
            let target_both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed primary-name claimed-name request failed")?;
            let sibling_declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=61&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("sibling declared primary-name claimed-name request failed")?;
            let sibling_both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=61&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("sibling mixed primary-name claimed-name request failed")?;

            assert_eq!(target_declared_response.status(), StatusCode::OK);
            assert_eq!(target_both_response.status(), StatusCode::OK);
            assert_eq!(sibling_declared_response.status(), StatusCode::OK);
            assert_eq!(sibling_both_response.status(), StatusCode::OK);

            let target_declared_payload: PrimaryNameResponse =
                read_json(target_declared_response).await?;
            let target_both_payload: PrimaryNameResponse = read_json(target_both_response).await?;
            let sibling_declared_payload: PrimaryNameResponse =
                read_json(sibling_declared_response).await?;
            let sibling_both_payload: PrimaryNameResponse = read_json(sibling_both_response).await?;
            let expected_claimed_primary_name = json!({
                "status": "success",
                "name": "alice.eth",
                "provenance": {
                    "source_family": "target_reverse",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            });
            let expected_sibling_claimed_primary_name = json!({
                "status": "success",
                "name": "beta.eth",
                "provenance": {
                    "source_family": "sibling_reverse",
                },
            });

            assert_eq!(target_declared_payload.data, target_expected_data);
            assert_eq!(target_both_payload.data, target_expected_data);
            assert_eq!(sibling_declared_payload.data, sibling_expected_data);
            assert_eq!(sibling_both_payload.data, sibling_expected_data);
            assert_eq!(
                target_declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": expected_claimed_primary_name.clone(),
                }))
            );
            assert_eq!(target_declared_payload.verified_state, None);
            assert_eq!(
                target_both_payload.declared_state,
                target_declared_payload.declared_state
            );
            assert_eq!(
                target_both_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );
            assert_eq!(
                sibling_declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": expected_sibling_claimed_primary_name.clone(),
                }))
            );
            assert_eq!(sibling_declared_payload.verified_state, None);
            assert_eq!(
                sibling_both_payload.declared_state,
                sibling_declared_payload.declared_state
            );
            assert_eq!(
                sibling_both_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            let target_claimed_primary_name = target_declared_payload
                .declared_state
                .as_ref()
                .and_then(|declared_state| declared_state.get("claimed_primary_name"))
                .and_then(Value::as_object)
                .expect("declared claimed_primary_name must be present");
            let provenance = target_claimed_primary_name
                .get("provenance")
                .and_then(Value::as_object)
                .expect("declared claimed_primary_name provenance must be present");
            assert!(!provenance.contains_key("execution_trace_id"));
            assert!(!provenance.contains_key("verified_primary_name_lookup"));
            assert!(!provenance.contains_key("verified_primary_name_invalidation"));
            assert_eq!(
                provenance.get("source_family"),
                Some(&json!("target_reverse"))
            );
            assert_eq!(
                target_claimed_primary_name.get("name"),
                Some(&json!("alice.eth"))
            );

            let sibling_claimed_primary_name = sibling_declared_payload
                .declared_state
                .as_ref()
                .and_then(|declared_state| declared_state.get("claimed_primary_name"))
                .and_then(Value::as_object)
                .expect("sibling declared claimed_primary_name must be present");
            assert_eq!(
                sibling_claimed_primary_name.get("name"),
                Some(&json!("beta.eth"))
            );

            for payload in [
                &target_declared_payload,
                &target_both_payload,
                &sibling_declared_payload,
                &sibling_both_payload,
            ] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_returns_not_found_results_by_mode_for_tuple_miss()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name tuple-miss request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name tuple-miss request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name tuple-miss request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "not_found",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_raw_claim_name_for_invalid_name_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            database
                .insert_primary_name_current_row(PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: PrimaryNameClaimStatus::InvalidName,
                    raw_claim_name: Some("alice..eth".to_owned()),
                    claim_provenance: json!({
                        "seed": "target_invalid_name",
                    }),
                })
                .await?;
            database
                .insert_primary_name_current_row(PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "61".to_owned(),
                    claim_status: PrimaryNameClaimStatus::InvalidName,
                    raw_claim_name: Some("sibling..eth".to_owned()),
                    claim_provenance: json!({
                        "seed": "sibling_invalid_name",
                    }),
                })
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared invalid-name primary-name request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified invalid-name primary-name request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed invalid-name primary-name request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "invalid_name",
                        "raw_claim_name": "alice..eth",
                        "provenance": {
                            "seed": "target_invalid_name",
                        },
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );
            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            let claimed_primary_name = both_payload
                .declared_state
                .as_ref()
                .and_then(|declared_state| declared_state.get("claimed_primary_name"))
                .and_then(Value::as_object)
                .expect("declared invalid-name payload must include claimed_primary_name");
            assert!(
                !claimed_primary_name.contains_key("name"),
                "declared invalid-name readback must not imply claimed_primary_name.name"
            );

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_persisted_verified_primary_name_for_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
            let finished_at = timestamp(1_717_172_401);
            let verified_primary_name = primary_name_verified_success(
                "ens:alice.eth",
                "alice.eth",
                "Alice.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000123",
                Uuid::from_u128(0x456),
            );

            seed_primary_name_tuple_anchor(&database, address, "60").await?;
            seed_primary_name_tuple_anchor(&database, address, "61").await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "ens",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "ens",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let other_finished_at = timestamp(1_717_172_499);
            let other_verified_primary_name = primary_name_verified_mismatch(
                "ens:other.eth",
                "other.eth",
                "other.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000456",
                Uuid::from_u128(0x999),
                "resolved_address_mismatch",
            );
            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
                    "ens",
                    address,
                    "61",
                    other_verified_primary_name.clone(),
                    other_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
                    "ens",
                    address,
                    "61",
                    other_verified_primary_name,
                    other_finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name persisted readback request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name persisted readback request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed primary-name persisted readback request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;
            let verified_section_provenance = json!({
                "manifest_versions": primary_name_execution_manifest_versions(),
                "execution_trace_id": execution_trace_id.to_string(),
            });
            let mut expected_verified_primary_name = verified_primary_name.clone();
            expected_verified_primary_name
                .as_object_mut()
                .expect("verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    verified_section_provenance.clone(),
                );

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_verified_primary_name,
                }))
            );
            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            assert_primary_name_unsupported_bootstrap_invariants(&declared_payload);
            assert_primary_name_supported_persisted_readback_invariants(
                &verified_payload,
                execution_trace_id,
                finished_at,
            );
            assert_primary_name_supported_persisted_readback_invariants(
                &both_payload,
                execution_trace_id,
                finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_persisted_basenames_verified_primary_name_for_exact_tuple()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bcd";
            let expected_data = json!({
                "address": address,
                "namespace": "basenames",
                "coin_type": "60",
            });
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004a);
            let finished_at = timestamp(1_717_172_410);
            let verified_primary_name = json!({
                "status": "success",
                "name": {
                    "logical_name_id": "basenames:alice.base.eth",
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
                    "resource_id": Uuid::from_u128(0x654).to_string(),
                    "binding_kind": "declared_registry_path",
                }
            });

            database
                .seed_basenames_primary_name_claim_observation(address, "60", "Alice.base.eth")
                .await?;
            database
                .rebuild_primary_names_current(address, "basenames", "60")
                .await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified basenames primary-name persisted readback request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed basenames primary-name persisted readback request failed")?;

            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;
            let verified_section_provenance = json!({
                "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
                "execution_trace_id": execution_trace_id.to_string(),
            });
            let mut expected_verified_primary_name = verified_primary_name.clone();
            expected_verified_primary_name
                .as_object_mut()
                .expect("Basenames verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    verified_section_provenance.clone(),
                );

            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_verified_primary_name,
                }))
            );
            assert_eq!(
                both_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "success",
                        "name": "alice.base.eth",
                        "provenance": {
                            "source_family": "basenames_base_primary",
                            "contract_role": "reverse_registrar",
                            "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                            "emitting_address": "0x00000000000000000000000000000000000000ad",
                        },
                    }
                }))
            );
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &verified_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );
            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &both_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_persisted_basenames_verified_primary_name_not_found_without_l1_resolver_call()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bce";
            let expected_data = json!({
                "address": address,
                "namespace": "basenames",
                "coin_type": "60",
            });
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004b);
            let finished_at = timestamp(1_717_172_411);
            let verified_primary_name = json!({
                "status": "not_found",
            });

            database
                .insert_primary_name_current_row(PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "basenames".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: PrimaryNameClaimStatus::NotFound,
                    raw_claim_name: None,
                    claim_provenance: json!({}),
                })
                .await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified basenames not_found primary-name request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed basenames not_found primary-name request failed")?;

            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;
            let verified_section_provenance = json!({
                "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
                "execution_trace_id": execution_trace_id.to_string(),
            });
            let mut expected_verified_primary_name = verified_primary_name.clone();
            expected_verified_primary_name
                .as_object_mut()
                .expect("Basenames not_found verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    verified_section_provenance.clone(),
                );

            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_verified_primary_name,
                }))
            );
            assert_eq!(
                both_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": {},
                    }
                }))
            );
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &verified_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );
            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &both_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reads_persisted_basenames_verified_primary_name_invalid_name_without_l1_resolver_call()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000bcf";
            let expected_data = json!({
                "address": address,
                "namespace": "basenames",
                "coin_type": "60",
            });
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004c);
            let finished_at = timestamp(1_717_172_412);
            let verified_primary_name = json!({
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable",
            });

            database
                .insert_primary_name_current_row(PrimaryNameCurrentRow {
                    address: address.to_owned(),
                    namespace: "basenames".to_owned(),
                    coin_type: "60".to_owned(),
                    claim_status: PrimaryNameClaimStatus::InvalidName,
                    raw_claim_name: Some("alice..base.eth".to_owned()),
                    claim_provenance: json!({}),
                })
                .await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await?;

            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified basenames invalid_name primary-name request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=basenames&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed basenames invalid_name primary-name request failed")?;

            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;
            let verified_section_provenance = json!({
                "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
                "execution_trace_id": execution_trace_id.to_string(),
            });
            let mut expected_verified_primary_name = verified_primary_name.clone();
            expected_verified_primary_name
                .as_object_mut()
                .expect("Basenames invalid_name verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    verified_section_provenance.clone(),
                );

            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);
            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_verified_primary_name,
                }))
            );
            assert_eq!(
                both_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "invalid_name",
                        "raw_claim_name": "alice..base.eth",
                        "provenance": {},
                    }
                }))
            );
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);
            let claimed_primary_name = both_payload
                .declared_state
                .as_ref()
                .and_then(|declared_state| declared_state.get("claimed_primary_name"))
                .and_then(Value::as_object)
                .expect("claimed_primary_name must be present");
            assert!(
                !claimed_primary_name.contains_key("name"),
                "invalid_name readback must not backfill claimed_primary_name.name"
            );

            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &verified_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );
            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                &both_payload,
                "basenames",
                execution_trace_id,
                finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_exact_tuple_invalidation_evicts_only_target_persisted_answer()
        -> Result<()> {
            for invalidation in [
                PersistedPrimaryNameInvalidation::Manifest,
                PersistedPrimaryNameInvalidation::Topology,
                PersistedPrimaryNameInvalidation::Record,
            ] {
                run_primary_name_execution_invalidation_coverage_case(invalidation).await?;
            }

            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_reports_unsupported_coverage_for_out_of_class_tuple_present()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";
            let expected_data = json!({
                "address": address,
                "namespace": "ens",
                "coin_type": "60",
            });

            database
                .seed_primary_name_reverse_changed(address, "60")
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", "60")
                .await?;

            let declared_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("declared primary-name tuple-present request failed")?;
            let verified_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("verified primary-name tuple-present request failed")?;
            let both_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("both primary-name tuple-present request failed")?;

            assert_eq!(declared_response.status(), StatusCode::OK);
            assert_eq!(verified_response.status(), StatusCode::OK);
            assert_eq!(both_response.status(), StatusCode::OK);

            let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
            let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
            let both_payload: PrimaryNameResponse = read_json(both_response).await?;

            assert_eq!(declared_payload.data, expected_data);
            assert_eq!(verified_payload.data, expected_data);
            assert_eq!(both_payload.data, expected_data);

            assert_eq!(
                declared_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(declared_payload.verified_state, None);

            assert_eq!(verified_payload.declared_state, None);
            assert_eq!(
                verified_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );

            assert_eq!(both_payload.declared_state, declared_payload.declared_state);
            assert_eq!(both_payload.verified_state, verified_payload.verified_state);

            for payload in [&declared_payload, &verified_payload, &both_payload] {
                assert_primary_name_unsupported_bootstrap_invariants(payload);
            }

            database.cleanup().await?;
            Ok(())
        }

        fn primary_name_supported_exact_tuple_coverage(namespace: &str) -> Value {
            let source_classes_considered = match namespace {
                "ens" => json!(["ens_v1_reverse_l1", "ens_execution"]),
                "basenames" => json!(["basenames_base_primary", "basenames_execution"]),
                other => panic!("unsupported primary-name coverage namespace {other}"),
            };

            json!({
                "status": "partial",
                "exhaustiveness": "non_enumerable",
                "source_classes_considered": source_classes_considered,
                "enumeration_basis": "primary_name_lookup",
                "unsupported_reason": null,
            })
        }

        fn primary_name_unsupported_exact_tuple_coverage() -> Value {
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [],
                "enumeration_basis": "primary_name_lookup",
                "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
            })
        }

        fn assert_primary_name_route_common_invariants_with_coverage(
            payload: &PrimaryNameResponse,
            expected_coverage: Value,
        ) {
            assert_eq!(payload.coverage, expected_coverage);
            assert_eq!(payload.chain_positions, json!({}));
            assert_eq!(payload.consistency, "head");
        }

        fn assert_primary_name_bootstrap_invariants_with_coverage(
            payload: &PrimaryNameResponse,
            expected_coverage: Value,
        ) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": [],
                    "execution_trace_id": null,
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants_with_coverage(
                payload,
                expected_coverage,
            );
            assert!(payload.last_updated.ends_with('Z'));
        }

        fn assert_primary_name_unsupported_bootstrap_invariants(
            payload: &PrimaryNameResponse,
        ) {
            assert_primary_name_bootstrap_invariants_with_coverage(
                payload,
                primary_name_unsupported_exact_tuple_coverage(),
            );
        }

        fn assert_primary_name_supported_persisted_readback_invariants_for_namespace(
            payload: &PrimaryNameResponse,
            namespace: &str,
            execution_trace_id: Uuid,
            finished_at: OffsetDateTime,
        ) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": primary_name_execution_manifest_versions_for_namespace(namespace),
                    "execution_trace_id": execution_trace_id.to_string(),
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants_with_coverage(
                payload,
                primary_name_supported_exact_tuple_coverage(namespace),
            );
            assert_eq!(payload.last_updated, format_timestamp(finished_at));
        }

        fn assert_primary_name_supported_persisted_readback_invariants(
            payload: &PrimaryNameResponse,
            execution_trace_id: Uuid,
            finished_at: OffsetDateTime,
        ) {
            assert_primary_name_supported_persisted_readback_invariants_for_namespace(
                payload,
                "ens",
                execution_trace_id,
                finished_at,
            );
        }

        async fn run_primary_name_execution_invalidation_coverage_case(
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture =
                seed_persisted_primary_name_execution_fixture(&database, invalidation).await?;
            let expected_data = json!({
                "address": fixture.address,
                "namespace": "ens",
                "coin_type": "60",
            });

            let verified_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;
            let both_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_before_response.status(), StatusCode::OK);
            assert_eq!(both_before_response.status(), StatusCode::OK);

            let verified_before_payload: PrimaryNameResponse =
                read_json(verified_before_response).await?;
            let both_before_payload: PrimaryNameResponse = read_json(both_before_response).await?;
            let mut expected_target_verified_primary_name =
                fixture.target_verified_primary_name.clone();
            expected_target_verified_primary_name
                .as_object_mut()
                .expect("target verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    json!({
                        "manifest_versions": primary_name_execution_manifest_versions(),
                        "execution_trace_id": fixture.target_execution_trace_id.to_string(),
                    }),
                );

            assert_eq!(verified_before_payload.data, expected_data);
            assert_eq!(both_before_payload.data, expected_data);
            assert_eq!(verified_before_payload.declared_state, None);
            assert_eq!(
                verified_before_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_target_verified_primary_name,
                }))
            );
            assert_eq!(
                both_before_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(
                both_before_payload.verified_state,
                verified_before_payload.verified_state
            );
            assert_primary_name_supported_persisted_readback_invariants(
                &verified_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );
            assert_primary_name_supported_persisted_readback_invariants(
                &both_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );

            invalidate_persisted_primary_name_execution(
                &database,
                &fixture.target_cache_key,
                invalidation,
            )
            .await?;

            assert_eq!(
                load_execution_outcome(&database.pool, &fixture.target_cache_key).await?,
                None
            );
            assert!(
                load_execution_trace(&database.pool, fixture.target_execution_trace_id)
                    .await?
                    .is_some(),
                "execution traces stay durable after verified-primary cache invalidation",
            );
            assert!(
                load_execution_outcome(&database.pool, &fixture.sibling_cache_key)
                    .await?
                    .is_some(),
                "exact-tuple invalidation must keep sibling tuple outcomes",
            );

            let verified_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let both_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let sibling_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=61&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "sibling primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_after_response.status(), StatusCode::OK);
            assert_eq!(both_after_response.status(), StatusCode::OK);
            assert_eq!(sibling_response.status(), StatusCode::OK);

            let verified_after_payload: PrimaryNameResponse =
                read_json(verified_after_response).await?;
            let both_after_payload: PrimaryNameResponse = read_json(both_after_response).await?;
            let sibling_payload: PrimaryNameResponse = read_json(sibling_response).await?;

            assert_eq!(verified_after_payload.data, expected_data);
            assert_eq!(both_after_payload.data, expected_data);
            assert_eq!(verified_after_payload.declared_state, None);
            assert_eq!(
                verified_after_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );
            assert_eq!(
                both_after_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(
                both_after_payload.verified_state,
                verified_after_payload.verified_state
            );
            assert_primary_name_unsupported_bootstrap_invariants(&verified_after_payload);
            assert_primary_name_unsupported_bootstrap_invariants(&both_after_payload);

            let mut expected_sibling_verified_primary_name =
                fixture.sibling_verified_primary_name.clone();
            expected_sibling_verified_primary_name
                .as_object_mut()
                .expect("sibling verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    json!({
                        "manifest_versions": primary_name_execution_manifest_versions(),
                        "execution_trace_id": invalidation
                            .sibling_execution_trace_id()
                            .to_string(),
                    }),
                );

            assert_eq!(
                sibling_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_sibling_verified_primary_name,
                }))
            );
            assert_primary_name_supported_persisted_readback_invariants(
                &sibling_payload,
                invalidation.sibling_execution_trace_id(),
                fixture.sibling_finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_requires_namespace_and_coin_type() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let address = "0x0000000000000000000000000000000000000abc";

            let missing_namespace = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/primary-names/{address}?coin_type=60"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("primary-name missing-namespace request failed")?;
            let missing_coin_type = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/primary-names/{address}?namespace=ens"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("primary-name missing-coin-type request failed")?;

            assert_eq!(missing_namespace.status(), StatusCode::BAD_REQUEST);
            assert_eq!(missing_coin_type.status(), StatusCode::BAD_REQUEST);

            let missing_namespace_payload: ErrorResponse = read_json(missing_namespace).await?;
            let missing_coin_type_payload: ErrorResponse = read_json(missing_coin_type).await?;

            assert_eq!(missing_namespace_payload.error.code, "invalid_input");
            assert_eq!(
                missing_namespace_payload.error.message,
                "namespace is required"
            );
            assert!(missing_namespace_payload.error.details.is_empty());

            assert_eq!(missing_coin_type_payload.error.code, "invalid_input");
            assert_eq!(
                missing_coin_type_payload.error.message,
                "coin_type is required"
            );
            assert!(missing_coin_type_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn primary_names_contract_rejects_invalid_tuples_and_unsupported_namespaces()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;

            let malformed_address = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/not-an-address?namespace=ens&coin_type=60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed primary-name address request failed")?;
            let malformed_coin_type = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60,61")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("malformed primary-name coin_type request failed")?;
            let unsupported_namespace = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=unknown&coin_type=60")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("unsupported primary-name namespace request failed")?;

            assert_eq!(malformed_address.status(), StatusCode::BAD_REQUEST);
            assert_eq!(malformed_coin_type.status(), StatusCode::BAD_REQUEST);
            assert_eq!(unsupported_namespace.status(), StatusCode::NOT_FOUND);

            let malformed_address_payload: ErrorResponse = read_json(malformed_address).await?;
            let malformed_coin_type_payload: ErrorResponse = read_json(malformed_coin_type).await?;
            let unsupported_namespace_payload: ErrorResponse =
                read_json(unsupported_namespace).await?;

            assert_eq!(malformed_address_payload.error.code, "invalid_input");
            assert_eq!(
                malformed_address_payload.error.message,
                "address must be a 0x-prefixed 20-byte hex string"
            );
            assert!(malformed_address_payload.error.details.is_empty());

            assert_eq!(malformed_coin_type_payload.error.code, "invalid_input");
            assert_eq!(
                malformed_coin_type_payload.error.message,
                "coin_type must contain only decimal digits"
            );
            assert!(malformed_coin_type_payload.error.details.is_empty());

            assert_eq!(unsupported_namespace_payload.error.code, "not_found");
            assert_eq!(
                unsupported_namespace_payload.error.message,
                "namespace unknown is not supported"
            );
            assert!(unsupported_namespace_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }
