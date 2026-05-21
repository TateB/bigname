const STANDARD_PROFILE_RECORD_KEYS: &[&str] =
    &["addr:60", "avatar", "text:com.twitter", "contenthash"];
const STANDARD_PROFILE_CACHE_RECORD_KEYS: &[&str] =
    &["addr:60", "text:com.twitter", "contenthash"];

#[tokio::test]
async fn get_resolution_execution_explain_returns_persisted_verified_state_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000021);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    let profile_request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let profile_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "avatar",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash"
        }
    ]);
    let profile_outcome = resolution_execution_outcome(
        execution_trace_id,
        &profile_request_key,
        profile_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    upsert_execution_outcome(&database.pool, &profile_outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    if resolution_response.status() != StatusCode::OK {
        let status = resolution_response.status();
        let payload: Value = read_json(resolution_response).await?;
        anyhow::bail!("expected profile response 200, got {status}: {payload}");
    }

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": profile_verified_queries
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, "finalized");
    assert_eq!(resolution_payload.consistency, "head");
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000025);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    seed_supported_alias_only_rebuild_inputs(
        &database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;
    let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("alias-only execution explain test requires rebuilt name_current row")?;
    let projected_topology = projected_resolution_topology(&name_row)?;
    let alias_target = projected_topology
        .pointer("/alias/final_target")
        .cloned()
        .context("alias-only projected topology must include final_target")?;
    let (topology_boundary, record_boundary) = projected_resolution_boundaries(&name_row)?;
    let mut inventory_row = record_inventory_current_row(logical_name_id, resource_id);
    inventory_row.record_version_boundary = record_boundary.clone();
    inventory_row.chain_positions = name_row.chain_positions.clone();
    database
        .insert_record_inventory_current_row(inventory_row.clone())
        .await?;

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
            "hops": [alias_target.clone()]
        }
    });
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions_from_name_current(&name_row.chain_positions),
    });
    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        topology_boundary.clone(),
        record_boundary.clone(),
    );
    outcome.cache_key.requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let profile_records = STANDARD_PROFILE_RECORD_KEYS
        .iter()
        .map(|record_key| {
            parse_resolution_record_key(record_key).expect("standard profile selector must parse")
        })
        .collect::<Vec<_>>();
    let profile_cache_records =
        bigname_storage::resolution_execution_cache_lookup_records(&name_row, &profile_records);
    let profile_cache_key = bigname_storage::build_resolution_execution_cache_key(
        &name_row,
        &profile_cache_records,
        Some(&inventory_row),
        name_row.chain_positions.clone(),
    )?;
    let profile_request_key = profile_cache_key.request_key.clone();
    let profile_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "not_found",
            "failure_reason": "no_addr_record"
        },
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash"
        }
    ]);
    let mut profile_outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &profile_request_key,
        profile_verified_queries.clone(),
        topology_boundary,
        record_boundary,
    );
    profile_outcome.cache_key = profile_cache_key;
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    upsert_execution_outcome(&database.pool, &profile_outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain alias request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution alias request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    if resolution_response.status() != StatusCode::OK {
        let status = resolution_response.status();
        let payload: Value = read_json(resolution_response).await?;
        anyhow::bail!("expected profile response 200, got {status}: {payload}");
    }

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": profile_verified_queries
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, "head");
    assert_eq!(resolution_payload.consistency, "head");
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": projected_topology.get("resolver_path").cloned().expect("projected topology must include resolver_path"),
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": alias_target.clone(),
                    "hops": [alias_target.clone()]
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice-via-alias.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

fn basenames_resolution_request_key(records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("basenames:alice.base.eth:{}", records.join(","))
}

fn requested_chain_positions_from_name_current(chain_positions: &Value) -> Value {
    let mut positions = chain_positions
        .as_object()
        .expect("name_current.chain_positions must be an object")
        .values()
        .map(|position| {
            json!({
                "chain_id": position
                    .get("chain_id")
                    .and_then(Value::as_str)
                    .expect("chain_position.chain_id must be present"),
                "block_number": position
                    .get("block_number")
                    .and_then(Value::as_i64)
                    .expect("chain_position.block_number must be present"),
                "block_hash": position
                    .get("block_hash")
                    .and_then(Value::as_str)
                    .expect("chain_position.block_hash must be present"),
            })
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        left.get("chain_id")
            .and_then(Value::as_str)
            .cmp(&right.get("chain_id").and_then(Value::as_str))
    });
    Value::Array(positions)
}

fn basenames_execution_manifest_version() -> Value {
    json!({
        "source_family": "basenames_execution",
        "manifest_version": 2,
        "chain": "ethereum-mainnet",
        "deployment_epoch": "basenames_v1",
    })
}

fn append_basenames_execution_manifest_version(name_row: &mut bigname_storage::NameCurrentRow) {
    let manifest_versions = name_row.provenance["manifest_versions"]
        .as_array_mut()
        .expect("name_current.provenance.manifest_versions must be an array");
    if manifest_versions.iter().any(|item| {
        item.get("source_family").and_then(Value::as_str) == Some("basenames_execution")
            && item.get("manifest_version").and_then(Value::as_i64) == Some(2)
    }) {
        return;
    }
    manifest_versions.push(basenames_execution_manifest_version());
}

fn insert_basenames_supported_ethereum_position(name_row: &mut bigname_storage::NameCurrentRow) {
    let chain_positions = name_row
        .chain_positions
        .as_object_mut()
        .expect("name_current.chain_positions must be an object");
    let authoritative_timestamp = chain_positions
        .values()
        .find(|position| position.get("chain_id").and_then(Value::as_str) == Some("base-mainnet"))
        .and_then(|position| position.get("timestamp"))
        .and_then(Value::as_str)
        .unwrap_or("2026-04-17T00:00:03Z")
        .to_owned();
    chain_positions.insert(
        "ethereum".to_owned(),
        json!({
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_100,
            "block_hash": "0xbasenamesl1",
            "timestamp": authoritative_timestamp,
        }),
    );
}

fn projected_resolution_topology(row: &bigname_storage::NameCurrentRow) -> Result<Value> {
    bigname_storage::projected_resolution_topology(&row.declared_summary)
        .context("rebuilt name_current row must project supported topology")
}

fn projected_resolution_boundaries(
    row: &bigname_storage::NameCurrentRow,
) -> Result<(Value, Value)> {
    let topology = projected_resolution_topology(row)?;
    bigname_storage::projected_resolution_boundaries_from_topology(&topology)
}

async fn seed_supported_alias_only_rebuild_inputs(
    database: &TestDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
            raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
            raw_block("ethereum-mainnet", "0xresolver", None, 101, 1_717_171_701),
            raw_block("ethereum-mainnet", "0xalias", None, 102, 1_717_171_702),
            raw_block(
                "ethereum-mainnet",
                "0xbinding-alias",
                None,
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(&database.pool, &[name_surface(logical_name_id)]).await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            token_lineage_id,
            "0xresource",
            99,
        )],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            "0xresource",
            99,
        )],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::ResolverAliasPath,
            active_from: timestamp(1_717_171_703),
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xbinding-alias".to_owned(),
            block_number: 103,
            provenance: json!({"seed": "supported_alias_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                event_identity: "api-test:alias-resolver".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(101),
                block_hash: Some("0xresolver".to_owned()),
                transaction_hash: Some("0xtxresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:alias-resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000abc",
                    "namehash": "namehash:alice.eth",
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:alias-changed".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "AliasChanged".to_owned(),
                source_family: "ens_v2_resolver".to_owned(),
                manifest_version: 5,
                source_manifest_id: None,
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(102),
                block_hash: Some("0xalias".to_owned()),
                transaction_hash: Some("0xtxalias".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:alias-changed"}),
                derivation_kind: "ens_v2_resolver".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "active": true,
                    "alias_state": "active",
                    "to_name": "profile.alice.eth",
                    "to_logical_name_id": "ens:profile.alice.eth",
                    "to_normalized_name": "profile.alice.eth",
                    "to_canonical_display_name": "Profile.alice.eth",
                    "to_namehash": "namehash:profile.alice.eth",
                    "to_resource_id": resource_id.to_string(),
                }),
            },
        ],
    )
    .await?;
    database.rebuild_name_current(logical_name_id).await
}

async fn seed_supported_wildcard_rebuild_inputs(
    database: &TestDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    wildcard_source_resource_id: Uuid,
) -> Result<()> {
    let wildcard_source_token_lineage_id = Uuid::from_u128(0x4401);
    let wildcard_source_binding_id = Uuid::from_u128(0x4402);

    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block(
                "ethereum-mainnet",
                "0xsource-surface",
                None,
                96,
                1_717_171_696,
            ),
            raw_block("ethereum-mainnet", "0xsurface", None, 98, 1_717_171_698),
            raw_block("ethereum-mainnet", "0xresource", None, 99, 1_717_171_699),
            raw_block(
                "ethereum-mainnet",
                "0xsource-resource",
                None,
                100,
                1_717_171_700,
            ),
            raw_block("ethereum-mainnet", "0xresolver", None, 101, 1_717_171_701),
            raw_block(
                "ethereum-mainnet",
                "0xsource-record-version",
                None,
                102,
                1_717_171_702,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xbinding-wildcard",
                None,
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[
            name_surface(logical_name_id),
            NameSurface {
                logical_name_id: "ens:eth".to_owned(),
                namespace: "ens".to_owned(),
                input_name: "eth".to_owned(),
                canonical_display_name: "Eth".to_owned(),
                normalized_name: "eth".to_owned(),
                dns_encoded_name: vec![3, b'e', b't', b'h'],
                namehash: "namehash:eth".to_owned(),
                labelhashes: vec!["labelhash:eth".to_owned()],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsource-surface".to_owned(),
                block_number: 96,
                provenance: json!({"seed": "supported_wildcard_source_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[
            address_name_token_lineage(token_lineage_id, "0xresource", 99),
            address_name_token_lineage(wildcard_source_token_lineage_id, "0xsource-resource", 100),
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[
            address_name_resource(resource_id, Some(token_lineage_id), "0xresource", 99),
            address_name_resource(
                wildcard_source_resource_id,
                Some(wildcard_source_token_lineage_id),
                "0xsource-resource",
                100,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[
            SurfaceBinding {
                surface_binding_id: wildcard_source_binding_id,
                logical_name_id: "ens:eth".to_owned(),
                resource_id: wildcard_source_resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsource-resource".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "supported_wildcard_source_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::ObservedWildcardPath,
                active_from: timestamp(1_717_171_703),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbinding-wildcard".to_owned(),
                block_number: 103,
                provenance: json!({"seed": "supported_wildcard_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                event_identity: "api-test:wildcard-source-resolver".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: Some("ens:eth".to_owned()),
                resource_id: Some(wildcard_source_resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(101),
                block_hash: Some("0xresolver".to_owned()),
                transaction_hash: Some("0xtxwildcardsourceresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:wildcard-source-resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000def",
                    "namehash": "namehash:eth",
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:wildcard-source-record-version".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: Some("ens:eth".to_owned()),
                resource_id: Some(wildcard_source_resource_id),
                event_kind: "RecordVersionChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(102),
                block_hash: Some("0xsource-record-version".to_owned()),
                transaction_hash: Some("0xtxwildcardsourceversion".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:wildcard-source-record-version"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({"record_version": 6}),
                after_state: json!({"record_version": 7}),
            },
        ],
    )
    .await?;
    database.rebuild_name_current(logical_name_id).await
}

async fn seed_supported_basenames_rebuild_inputs(
    database: &TestDatabase,
    logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    bigname_storage::upsert_raw_blocks(
        &database.pool,
        &[
            raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
            raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
            raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
            raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
            raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
            raw_block(
                "base-mainnet",
                "0xbase-binding-supported",
                None,
                104,
                1_717_171_704,
            ),
            raw_block(
                "ethereum-mainnet",
                "0xbasenamesl1",
                None,
                21_000_100,
                1_776_387_700,
            ),
        ],
    )
    .await?;
    insert_chain_checkpoint(database, "ethereum-mainnet", "0xbasenamesl1", 21_000_100).await?;
    bigname_storage::upsert_name_surfaces(
        &database.pool,
        &[NameSurface {
            logical_name_id: logical_name_id.to_owned(),
            namespace: "basenames".to_owned(),
            input_name: "alice.base.eth".to_owned(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: "alice.base.eth".to_owned(),
            dns_encoded_name: b"alice.base.eth".to_vec(),
            namehash: "namehash:alice.base.eth".to_owned(),
            labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 98,
            provenance: json!({"seed": "supported_basenames_surface"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        &database.pool,
        &[TokenLineage {
            token_lineage_id,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "supported_basenames_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        &database.pool,
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "supported_basenames_resource"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        &database.pool,
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_171_704),
            active_to: None,
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbase-binding-supported".to_owned(),
            block_number: 104,
            provenance: json!({"seed": "supported_basenames_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        &database.pool,
        &[
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:grant".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "RegistrationGranted".to_owned(),
                source_family: "basenames_base_registrar".to_owned(),
                manifest_version: 3,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(101),
                block_hash: Some("0xbase-grant".to_owned()),
                transaction_hash: Some("0xtxbasegrant".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:grant"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:alice",
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "expiry": 1_900_000_000_i64,
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:authority".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "AuthorityTransferred".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                manifest_version: 3,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(102),
                block_hash: Some("0xbase-authority".to_owned()),
                transaction_hash: Some("0xtxbaseauthority".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:authority"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "owner": "0x00000000000000000000000000000000000000bb",
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:resolver".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(103),
                block_hash: Some("0xbase-resolver".to_owned()),
                transaction_hash: Some("0xtxbaseresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000abc",
                    "namehash": "namehash:alice.base.eth",
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:record-version".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "RecordVersionChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbaserecordversion".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:record-version"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({
                    "record_version": 6,
                }),
                after_state: json!({
                    "record_version": 7,
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:addr".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "RecordChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbaseaddr".to_owned()),
                log_index: Some(1),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:addr"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                }),
            },
            NormalizedEvent {
                event_identity: "api-test:supported-basenames:text".to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(logical_name_id.to_owned()),
                resource_id: Some(resource_id),
                event_kind: "RecordChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbasetext".to_owned()),
                log_index: Some(2),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "api-test:supported-basenames:text"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                }),
            },
        ],
    )
    .await?;
    let manifest_id = database
        .insert_manifest(
            "basenames",
            "basenames_execution",
            "ethereum-mainnet",
            "basenames_v1",
            2,
            "active",
            "ensip15@2026-04-16",
        )
        .await?;
    database
        .insert_capability_flag(manifest_id, "verified_resolution", "supported", None)
        .await?;
    insert_basenames_execution_manifest_contract(database, manifest_id).await?;
    database.rebuild_name_current(logical_name_id).await?;
    let row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("supported Basenames rebuild input must create name_current row")?;
    database
        .seed_snapshot_selector_chain_positions(&row.chain_positions)
        .await
}

async fn insert_chain_checkpoint(
    database: &TestDatabase,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id)
        DO UPDATE SET
            finalized_block_hash = EXCLUDED.finalized_block_hash,
            finalized_block_number = EXCLUDED.finalized_block_number
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(&database.pool)
    .await
    .with_context(|| format!("failed to insert chain checkpoint for {chain_id}"))?;

    Ok(())
}

async fn insert_basenames_execution_manifest_contract(
    database: &TestDatabase,
    manifest_id: i64,
) -> Result<()> {
    let contract_instance_id = Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0002);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, 'ethereum-mainnet', 'contract', $2::jsonb)
        ON CONFLICT (contract_instance_id) DO NOTHING
        "#,
    )
    .bind(contract_instance_id)
    .bind(json!({"seed": "api_resolution_basenames_execution"}))
    .execute(&database.pool)
    .await
    .context("failed to insert Basenames execution contract_instance for API resolution test")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES (
            $1,
            'contract',
            'l1_resolver',
            $2,
            '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31',
            'l1_resolver',
            'none'
        )
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .execute(&database.pool)
    .await
    .context(
        "failed to insert Basenames execution manifest_contract_instance for API resolution test",
    )?;

    Ok(())
}

fn resolution_unsupported_verified_state(records: &[&str]) -> Value {
    json!({
        "verified_queries": records
            .iter()
            .map(|record_key| {
                json!({
                    "record_key": record_key,
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn ensv2_sepolia_resolution_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
            "chain_id": "ethereum-sepolia",
            "block_number": 206,
            "block_hash": "0xensv2-regen",
            "timestamp": "2024-05-31T19:03:26Z",
        },
    })
}

fn ensv2_sepolia_resolution_topology(
    logical_name_id: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    boundary: &Value,
) -> Value {
    json!({
        "registry_path": [{
            "logical_name_id": logical_name_id,
            "namespace": "ens",
            "normalized_name": normalized_name,
            "canonical_display_name": canonical_display_name,
            "namehash": format!("namehash:{normalized_name}"),
            "resource_id": resource_id.to_string(),
            "binding_kind": "declared_registry_path",
        }],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "namespace": "ens",
            "normalized_name": normalized_name,
            "canonical_display_name": canonical_display_name,
            "resource_id": resource_id.to_string(),
            "chain_id": "ethereum-sepolia",
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
            "topology_version_boundary": boundary.clone(),
            "record_version_boundary": boundary.clone(),
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        },
    })
}

fn ensv2_sepolia_resolution_row(
    logical_name_id: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    include_projected_topology: bool,
) -> bigname_storage::NameCurrentRow {
    let boundary = ensv2_sepolia_resolution_boundary(logical_name_id, resource_id);
    let mut declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "ens_v2_registry",
            "latest_event_kind": "RegistrationRenewed",
        },
        "resolver": {
            "chain_id": "ethereum-sepolia",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged",
        },
    });
    if include_projected_topology {
        declared_summary["topology"] = ensv2_sepolia_resolution_topology(
            logical_name_id,
            normalized_name,
            canonical_display_name,
            resource_id,
            &boundary,
        );
    }

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.canonical_display_name = canonical_display_name.to_owned();
    row.normalized_name = normalized_name.to_owned();
    row.namehash = format!("namehash:{normalized_name}");
    row.declared_summary = declared_summary;
    row.provenance = json!({
        "normalized_event_ids": [204, 205, 206],
        "raw_fact_refs": [{
            "kind": "raw_log",
            "chain_id": "ethereum-sepolia",
            "block_number": 206,
        }],
        "manifest_versions": [
            {
                "manifest_version": 11,
                "source_family": "ens_v2_registry_l1",
                "chain": "ethereum-sepolia",
                "deployment_epoch": "ens_v2_sepolia_dev",
            },
            {
                "manifest_version": 11,
                "source_family": "ens_v2_registrar_l1",
                "chain": "ethereum-sepolia",
                "deployment_epoch": "ens_v2_sepolia_dev",
            }
        ],
        "execution_trace_id": null,
        "derivation_kind": "name_current_rebuild",
    });
    row.coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["ens_v2_registry_l1", "ens_v2_registrar_l1"],
        "unsupported_reason": null,
        "enumeration_basis": "exact_name_profile",
    });
    row.chain_positions = json!({
        "ethereum-sepolia": {
            "chain_id": "ethereum-sepolia",
            "block_number": 206,
            "block_hash": "0xensv2-regen",
            "timestamp": "2024-05-31T19:03:26Z",
        }
    });
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "ethereum-sepolia": "finalized",
        }
    });
    row.manifest_version = 11;
    row.last_recomputed_at = timestamp(1_717_171_906);
    row
}

fn ensv2_sepolia_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: ensv2_sepolia_resolution_boundary(logical_name_id, resource_id),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true,
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "cacheable": true,
            }
        ]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1206,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "ethereum-sepolia",
                "block_number": 206,
                "block_hash": "0xensv2-regen",
                "timestamp": "2024-05-31T19:03:26Z",
            }
        })),
        entries: json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000abc",
                }
            },
            {
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice-sepolia",
                }
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1206],
            "derivation_kind": "record_inventory_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v2_resolver_l1"],
            "unsupported_reason": null,
            "enumeration_basis": "declared_record_inventory",
        }),
        chain_positions: json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 206,
                "block_hash": "0xensv2-regen",
                "timestamp": "2024-05-31T19:03:26Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-sepolia": "finalized",
            }
        }),
        manifest_version: 11,
        last_recomputed_at: timestamp(1_717_171_907),
    }
}

async fn seed_resolution_route_name_current(
    database: &TestDatabase,
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    let logical_name_id = format!("{namespace}:{normalized_name}");
    let namehash = format!("namehash:{normalized_name}");

    database
        .seed_name_current_binding(
            &logical_name_id,
            namespace,
            normalized_name,
            canonical_display_name,
            &namehash,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(resolution_route_name_row(
            namespace,
            normalized_name,
            canonical_display_name,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        ))
        .await
}

fn resolution_route_name_row(
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    let logical_name_id = format!("{namespace}:{normalized_name}");
    let mut row = exact_name_row(
        &logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.namespace = namespace.to_owned();
    row.normalized_name = normalized_name.to_owned();
    row.canonical_display_name = canonical_display_name.to_owned();
    row.namehash = format!("namehash:{normalized_name}");

    if namespace == "basenames" {
        row.declared_summary = json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": basenames_exact_name_resolver_summary()
        });
        row.provenance = json!({
            "normalized_event_ids": [201, 202],
            "raw_fact_refs": [
                {
                    "kind": "log",
                    "chain_id": "base-mainnet",
                    "block_hash": "0xbase-binding"
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 4,
                    "source_family": "basenames_base_resolver",
                    "chain": "base-mainnet",
                    "deployment_epoch": "basenames_v1"
                }
            ],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        });
        row.coverage = json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["basenames_base_registry"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        });
        row.chain_positions = json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        });
        row.canonicality_summary = json!({
            "status": "finalized",
            "chains": {
                "base-mainnet": "finalized"
            }
        });
        row.manifest_version = 4;
    }

    row
}

fn resolution_request_key_for(namespace: &str, normalized_name: &str, records: &[&str]) -> String {
    let mut records = records
        .iter()
        .map(|record| (*record).to_owned())
        .collect::<Vec<_>>();
    records.sort_unstable();
    format!("{namespace}:{normalized_name}:{}", records.join(","))
}

#[tokio::test]
async fn get_resolution_inferred_route_infers_base_eth_as_ens() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x7100);
    let token_lineage_id = Uuid::from_u128(0x7101);
    let surface_binding_id = Uuid::from_u128(0x7102);

    seed_resolution_route_name_current(
        &database,
        "ens",
        "base.eth",
        "Base.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    let inferred_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("inferred request must build"),
        )
        .await
        .context("inferred base.eth resolution request failed")?;
    let canonical_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("canonical request must build"),
        )
        .await
        .context("canonical base.eth resolution request failed")?;

    assert_eq!(inferred_response.status(), StatusCode::OK);
    assert_eq!(canonical_response.status(), StatusCode::OK);

    let inferred_payload: ResolutionResponse = read_json(inferred_response).await?;
    let canonical_payload: ResolutionResponse = read_json(canonical_response).await?;
    assert_eq!(inferred_payload, canonical_payload);
    assert_eq!(inferred_payload.data.get("namespace"), Some(&json!("ens")));
    assert_eq!(
        inferred_payload.data.get("logical_name_id"),
        Some(&json!("ens:base.eth"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_inferred_route_infers_non_base_eth_name_as_ens() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x7110);
    let token_lineage_id = Uuid::from_u128(0x7111);
    let surface_binding_id = Uuid::from_u128(0x7112);

    seed_resolution_route_name_current(
        &database,
        "ens",
        "alice.eth",
        "Alice.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    let inferred_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/Alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("inferred request must build"),
        )
        .await
        .context("inferred alice.eth resolution request failed")?;
    let canonical_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("canonical request must build"),
        )
        .await
        .context("canonical alice.eth resolution request failed")?;

    assert_eq!(inferred_response.status(), StatusCode::OK);
    assert_eq!(canonical_response.status(), StatusCode::OK);

    let inferred_payload: ResolutionResponse = read_json(inferred_response).await?;
    let canonical_payload: ResolutionResponse = read_json(canonical_response).await?;
    assert_eq!(inferred_payload, canonical_payload);
    assert_eq!(inferred_payload.data.get("namespace"), Some(&json!("ens")));
    assert_eq!(
        inferred_payload.data.get("logical_name_id"),
        Some(&json!("ens:alice.eth"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_inferred_route_rejects_unnormalizable_name() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/bad%20name.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("inferred invalid-name request must build"),
        )
        .await
        .context("inferred invalid-name resolution request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_inferred_route_infers_child_base_eth_as_basenames() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x7200);
    let token_lineage_id = Uuid::from_u128(0x7201);
    let surface_binding_id = Uuid::from_u128(0x7202);

    seed_resolution_route_name_current(
        &database,
        "basenames",
        "alice.base.eth",
        "Alice.base.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    let inferred_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("inferred request must build"),
        )
        .await
        .context("inferred alice.base.eth resolution request failed")?;
    let canonical_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("canonical request must build"),
        )
        .await
        .context("canonical alice.base.eth resolution request failed")?;

    assert_eq!(inferred_response.status(), StatusCode::OK);
    assert_eq!(canonical_response.status(), StatusCode::OK);

    let inferred_payload: ResolutionResponse = read_json(inferred_response).await?;
    let canonical_payload: ResolutionResponse = read_json(canonical_response).await?;
    assert_eq!(inferred_payload, canonical_payload);
    assert_eq!(
        inferred_payload.data.get("namespace"),
        Some(&json!("basenames"))
    );
    assert_eq!(
        inferred_payload.data.get("logical_name_id"),
        Some(&json!("basenames:alice.base.eth"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_inferred_basenames_verified_does_not_fallback_to_ens() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let ens_logical_name_id = "ens:alice.base.eth";
    let ens_resource_id = Uuid::from_u128(0x7300);
    let ens_token_lineage_id = Uuid::from_u128(0x7301);
    let ens_surface_binding_id = Uuid::from_u128(0x7302);
    let basenames_resource_id = Uuid::from_u128(0x7400);
    let basenames_token_lineage_id = Uuid::from_u128(0x7401);
    let basenames_surface_binding_id = Uuid::from_u128(0x7402);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000040);
    let request_key = resolution_request_key_for("ens", "alice.base.eth", &["addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    seed_resolution_route_name_current(
        &database,
        "ens",
        "alice.base.eth",
        "Alice.base.eth",
        ens_resource_id,
        ens_token_lineage_id,
        ens_surface_binding_id,
    )
    .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            ens_logical_name_id,
            ens_resource_id,
        ))
        .await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.request_metadata["surface"] = json!("alice.base.eth");
    if let Some(call_step) = trace
        .steps
        .iter_mut()
        .find(|step| step.step_kind == "call_universal_resolver")
    {
        call_step.step_payload["name"] = json!("alice.base.eth");
        call_step.step_payload["record_count"] = json!(1);
    }
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        ens_logical_name_id,
        ens_resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    seed_resolution_route_name_current(
        &database,
        "basenames",
        "alice.base.eth",
        "Alice.base.eth",
        basenames_resource_id,
        basenames_token_lineage_id,
        basenames_surface_binding_id,
    )
    .await?;

    let inferred_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("inferred request must build"),
        )
        .await
        .context("inferred alice.base.eth verified resolution request failed")?;

    assert_eq!(inferred_response.status(), StatusCode::CONFLICT);

    let inferred_error: ErrorResponse = read_json(inferred_response).await?;
    assert_eq!(inferred_error.error.code, "stale");
    assert_eq!(
        inferred_error.error.message,
        "name_current projection does not match the selected snapshot"
    );

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

#[test]
fn basenames_selected_superset_requires_authoritative_base_inventory_position() {
    let base_position = json!({
        "chain_id": "base-mainnet",
        "block_number": 21_000_003,
        "block_hash": "0xbase-binding",
        "timestamp": "2026-04-17T00:00:03Z",
    });
    let ethereum_position = json!({
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_100,
        "block_hash": "0xbasenamesl1",
        "timestamp": "2026-04-17T00:00:03Z",
    });
    let selected_snapshot = SelectedSnapshot {
        chain_positions: ChainPositions::from_value(&json!({
            "base": base_position.clone(),
            "ethereum": ethereum_position.clone(),
        }))
        .expect("selected chain positions must parse"),
        consistency: SnapshotConsistency::Finalized,
    };

    for projected in [
        json!({}),
        json!({
            "ethereum": ethereum_position.clone(),
        }),
        json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xbase-other",
                "timestamp": "2026-04-17T00:00:04Z",
            }
        }),
    ] {
        let projected = ChainPositions::from_value(&projected)
            .expect("projected chain positions must parse");
        assert!(!crate::handler_resolution::record_inventory_chain_positions_match_selected_snapshot(
            &projected,
            &selected_snapshot,
            true,
        ));
    }

    let projected = ChainPositions::from_value(&json!({
        "base-mainnet": base_position,
    }))
    .expect("projected base-only chain position must parse");
    assert!(crate::handler_resolution::record_inventory_chain_positions_match_selected_snapshot(
        &projected,
        &selected_snapshot,
        true,
    ));
}

#[test]
fn basenames_legacy_support_requires_explicit_execution_chain_and_epoch() {
    let mut row = exact_name_row(
        "basenames:alice.base.eth",
        Uuid::from_u128(0x6a00),
        Uuid::from_u128(0x6a01),
        Uuid::from_u128(0x6a02),
    );
    row.namespace = "basenames".to_owned();
    row.normalized_name = "alice.base.eth".to_owned();
    row.canonical_display_name = "Alice.base.eth".to_owned();
    row.namehash = "namehash:alice.base.eth".to_owned();
    row.declared_summary["resolver"]["chain_id"] = json!("base-mainnet");
    row.provenance["manifest_versions"] = json!([basenames_execution_manifest_version()]);
    row.chain_positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z",
        },
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_100,
            "block_hash": "0xbasenamesl1",
            "timestamp": "2026-04-17T00:00:03Z",
        }
    });
    assert!(bigname_storage::resolution_verified_support_boundary(&row, None).is_some());

    let mut missing_chain = row.clone();
    missing_chain.provenance["manifest_versions"][0]
        .as_object_mut()
        .expect("manifest version must be an object")
        .remove("chain");
    assert!(
        bigname_storage::resolution_verified_support_boundary(&missing_chain, None).is_none()
    );

    let mut wrong_chain = row.clone();
    wrong_chain.provenance["manifest_versions"][0]["chain"] = json!("base-mainnet");
    assert!(bigname_storage::resolution_verified_support_boundary(&wrong_chain, None).is_none());

    let mut missing_epoch = row.clone();
    missing_epoch.provenance["manifest_versions"][0]
        .as_object_mut()
        .expect("manifest version must be an object")
        .remove("deployment_epoch");
    assert!(
        bigname_storage::resolution_verified_support_boundary(&missing_epoch, None).is_none()
    );

    let mut wrong_epoch = row;
    wrong_epoch.provenance["manifest_versions"][0]["deployment_epoch"] = json!("basenames_v2");
    assert!(bigname_storage::resolution_verified_support_boundary(&wrong_epoch, None).is_none());
}

fn basenames_no_declared_resolver_topology(
    logical_name_id: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Value {
    json!({
        "registry_path": [basenames_name_ref(
            logical_name_id,
            normalized_name,
            canonical_display_name,
            resource_id,
            "declared_registry_path",
        )],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "namespace": "basenames",
            "normalized_name": normalized_name,
            "canonical_display_name": canonical_display_name,
            "resource_id": resource_id.to_string(),
            "chain_id": null,
            "address": null,
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

fn basenames_dynamic_resolver_record_inventory_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z",
        }
    })
}

fn basenames_l2resolver_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: basenames_dynamic_resolver_record_inventory_boundary(
            logical_name_id,
            resource_id,
            Some(1201),
            Some("RecordChanged"),
        ),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: json!([
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "cacheable": true,
            }
        ]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1201,
            "event_kind": "RecordChanged",
            "chain_position": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        })),
        entries: json!([
            {
                "record_key": "text",
                "record_family": "text",
                "selector_key": null,
                "status": "unsupported",
                "unsupported_reason": "value_not_retained_in_normalized_events",
            }
        ]),
        provenance: json!({
            "normalized_event_ids": [1201],
            "derivation_kind": "record_inventory_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [
                "basenames_base_registry",
                "basenames_base_resolver",
            ],
            "unsupported_reason": null,
            "enumeration_basis": "declared_record_inventory",
        }),
        chain_positions: json!({
            "base-mainnet": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "base-mainnet": "finalized",
            }
        }),
        manifest_version: 6,
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

fn basenames_dynamic_resolver_pending_record_inventory_current_row(
    logical_name_id: &str,
    resource_id: Uuid,
) -> bigname_storage::RecordInventoryCurrentRow {
    bigname_storage::RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: basenames_dynamic_resolver_record_inventory_boundary(
            logical_name_id,
            resource_id,
            Some(1202),
            Some("ResolverChanged"),
        ),
        enumeration_basis: json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: json!([]),
        explicit_gaps: json!([]),
        unsupported_families: json!([
            {
                "record_family": "addr",
                "unsupported_reason": "resolver_family_pending",
            },
            {
                "record_family": "text",
                "unsupported_reason": "resolver_family_pending",
            }
        ]),
        last_change: Some(json!({
            "normalized_event_id": 1202,
            "event_kind": "ResolverChanged",
            "chain_position": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        })),
        entries: json!([]),
        provenance: json!({
            "normalized_event_ids": [1202],
            "derivation_kind": "record_inventory_current_rebuild",
        }),
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "best_effort",
            "source_classes_considered": ["basenames_base_registry"],
            "unsupported_reason": "resolver_family_pending",
            "enumeration_basis": "declared_record_inventory",
        }),
        chain_positions: json!({
            "base-mainnet": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z",
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "base-mainnet": "finalized",
            }
        }),
        manifest_version: 6,
        last_recomputed_at: timestamp(1_717_171_720),
    }
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
                Uuid::from_u128(0x6201),
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
                Uuid::from_u128(0x6202),
                "observed_wildcard_path",
            );
            topology["resolver_path"] = json!([basenames_resolver_hop(
                "basenames:wild.base.eth",
                "wild.base.eth",
                "Wild.base.eth",
                Uuid::from_u128(0x6202),
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
                Uuid::from_u128(0x6203),
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
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6210);
    let token_lineage_id = Uuid::from_u128(0x6211);
    let surface_binding_id = Uuid::from_u128(0x6212);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000039);
    let request_key = basenames_resolution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record",
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
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
        .context("basenames declared resolution must expose record_inventory boundary")?;
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
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_inventory_boundary,
    )
    .await?
    .context(
        "worker-produced basenames record_inventory_current row must exist after transport seed",
    )?;

    let requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.request_key = request_key.clone();
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions.clone(),
    });
    trace.manifest_context = json!({
        "manifest_versions": [{
            "source_family": "basenames_execution",
            "manifest_version": 2
        }]
    });
    trace.contracts_called = json!([
        {
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "selector": "0x9061b923"
        }
    ]);
    trace.gateway_digests = json!(["sha256:ccip-request", "sha256:ccip-response"]);
    trace.request_metadata = json!({
        "surface": "alice.base.eth",
        "record_keys": ["text:com.twitter", "addr:60"],
        "entrypoint": "l1_resolver",
        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "latest_event_kind": null
        }
    });
    trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_declared_topology".to_owned(),
            input_digest: Some("sha256:topology-input".to_owned()),
            output_digest: Some("sha256:topology-output".to_owned()),
            latency_ms: Some(4),
            canonicality_dependency: json!({
                "base-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "entrypoint": "l1_resolver",
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "call_l1_resolver".to_owned(),
            input_digest: Some("sha256:l1-input".to_owned()),
            output_digest: Some("sha256:l1-output".to_owned()),
            latency_ms: Some(17),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "name": "alice.base.eth",
                "record_count": 2
            }),
        },
        ExecutionTraceStep {
            step_index: 2,
            step_kind: "ccip_offchain_lookup".to_owned(),
            input_digest: Some("sha256:ccip-input".to_owned()),
            output_digest: Some("sha256:ccip-output".to_owned()),
            latency_ms: Some(29),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "gateway_digest": "sha256:ccip-request"
            }),
        },
        ExecutionTraceStep {
            step_index: 3,
            step_kind: "resolve_with_proof".to_owned(),
            input_digest: Some("sha256:proof-input".to_owned()),
            output_digest: Some("sha256:proof-output".to_owned()),
            latency_ms: Some(11),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "proof_kind": "signature"
            }),
        },
    ];

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        worker_row.record_version_boundary.clone(),
        worker_row.record_version_boundary.clone(),
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.requested_chain_positions = requested_chain_positions;
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=both&meta=full")
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
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60")
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
        Some(resolution_unsupported_verified_state(&["addr:60", "text"])),
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
async fn get_resolution_both_mode_reads_persisted_basenames_transport_direct_answers() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6200);
    let token_lineage_id = Uuid::from_u128(0x6100);
    let surface_binding_id = Uuid::from_u128(0x6300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000033);
    let request_key = basenames_resolution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record",
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    seed_supported_basenames_rebuild_inputs(
        &database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(
                    "/v1/profiles/names/alice.base.eth?mode=declared&meta=full",
                )
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames declared resolution request failed before transport assertions")?;
    assert_eq!(declared_response.status(), StatusCode::OK);

    let declared_payload: ResolutionResponse = read_json(declared_response).await?;
    let record_inventory_boundary = declared_payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("record_inventory"))
        .and_then(|value| value.get("record_version_boundary"))
        .cloned()
        .context("basenames declared resolution must expose record_inventory boundary")?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_inventory_boundary,
    )
    .await?
    .context("worker-produced basenames record_inventory_current row must exist")?;
    let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("basenames supported resolution test requires rebuilt name_current row")?;
    append_basenames_execution_manifest_version(&mut name_row);
    insert_basenames_supported_ethereum_position(&mut name_row);
    let topology = basenames_supported_topology(
        logical_name_id,
        resource_id,
        &worker_row.record_version_boundary,
    );
    name_row.declared_summary["topology"] = topology.clone();
    database.insert_name_current_row(name_row.clone()).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let (topology_boundary, record_boundary) =
        bigname_storage::projected_resolution_boundaries_from_topology(&topology)?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_boundary,
    )
    .await?
    .context("worker-produced basenames record_inventory_current row must exist")?;
    let requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.request_key = request_key.clone();
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions.clone(),
    });
    trace.manifest_context = json!({
        "manifest_versions": [{
            "source_family": "basenames_execution",
            "manifest_version": 2
        }]
    });
    trace.contracts_called = json!([
        {
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "selector": "0x9061b923"
        }
    ]);
    trace.gateway_digests = json!(["sha256:ccip-request", "sha256:ccip-response"]);
    trace.request_metadata = json!({
        "surface": "alice.base.eth",
        "record_keys": ["text:com.twitter", "addr:60"],
        "entrypoint": "l1_resolver",
        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        "transport": topology.get("transport").cloned().expect("projected topology must include transport")
    });
    trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_declared_topology".to_owned(),
            input_digest: Some("sha256:topology-input".to_owned()),
            output_digest: Some("sha256:topology-output".to_owned()),
            latency_ms: Some(4),
            canonicality_dependency: json!({
                "base-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "entrypoint": "l1_resolver",
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "call_l1_resolver".to_owned(),
            input_digest: Some("sha256:l1-input".to_owned()),
            output_digest: Some("sha256:l1-output".to_owned()),
            latency_ms: Some(17),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "name": "alice.base.eth",
                "record_count": 2
            }),
        },
        ExecutionTraceStep {
            step_index: 2,
            step_kind: "ccip_offchain_lookup".to_owned(),
            input_digest: Some("sha256:ccip-input".to_owned()),
            output_digest: Some("sha256:ccip-output".to_owned()),
            latency_ms: Some(29),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "gateway_digest": "sha256:ccip-request"
            }),
        },
        ExecutionTraceStep {
            step_index: 3,
            step_kind: "resolve_with_proof".to_owned(),
            input_digest: Some("sha256:proof-input".to_owned()),
            output_digest: Some("sha256:proof-output".to_owned()),
            latency_ms: Some(11),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "proof_kind": "signature"
            }),
        },
    ];

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        topology_boundary,
        record_boundary.clone(),
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.requested_chain_positions = requested_chain_positions.clone();
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let mut profile_outcome = outcome.clone();
    profile_outcome.cache_key.request_key = basenames_resolution_request_key(&["addr:60"]);
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    upsert_execution_outcome(&database.pool, &profile_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(
                    "/v1/profiles/names/alice.base.eth?mode=both&meta=full",
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
        .context("basenames execution explain request failed")?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let payload: Value = read_json(response).await?;
        anyhow::bail!("expected basenames profile response 200, got {status}: {payload}");
    }
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
                }
            ],
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
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
                },
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported"
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
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "basenames_execution",
                    "role": "l1_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"
                },
                "resolver_discovery_path": topology.get("resolver_path").cloned().expect("topology must include resolver_path"),
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "base-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_l1_resolver",
                        "input_digest": "sha256:l1-input",
                        "output_digest": "sha256:l1-output",
                        "latency": 17,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 2,
                        "step_kind": "ccip_offchain_lookup",
                        "input_digest": "sha256:ccip-input",
                        "output_digest": "sha256:ccip-output",
                        "latency": 29,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 3,
                        "step_kind": "resolve_with_proof",
                        "input_digest": "sha256:proof-input",
                        "output_digest": "sha256:proof-output",
                        "latency": 11,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": persisted_verified_queries
        }))
    );

    let mut unsupported_worker_row = worker_row.clone();
    unsupported_worker_row.coverage["unsupported_reason"] = json!("resolver_family_pending");
    database
        .insert_record_inventory_current_row(unsupported_worker_row)
        .await?;
    let unsupported_explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pending-inventory basenames execution explain request failed")?;
    assert_eq!(unsupported_explain_response.status(), StatusCode::OK);
    let unsupported_explain_payload: ResolutionResponse =
        read_json(unsupported_explain_response).await?;
    assert_eq!(
        unsupported_explain_payload.verified_state,
        explain_payload.verified_state
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_keeps_basenames_transport_explicit_without_ethereum_position() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6404);
    let token_lineage_id = Uuid::from_u128(0x6504);
    let surface_binding_id = Uuid::from_u128(0x6604);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000035);
    let request_key = basenames_resolution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record",
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames declared resolution request failed before missing-ethereum assertions")?;
    assert_eq!(declared_response.status(), StatusCode::OK);
    let declared_payload: ResolutionResponse = read_json(declared_response).await?;
    let record_inventory_boundary = declared_payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("record_inventory"))
        .and_then(|value| value.get("record_version_boundary"))
        .cloned()
        .context("basenames declared resolution must expose record_inventory boundary")?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_inventory_boundary,
    )
    .await?
    .context("worker-produced basenames record_inventory_current row must exist")?;
    let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("basenames missing-ethereum test requires name_current row")?;
    append_basenames_execution_manifest_version(&mut name_row);
    name_row.declared_summary["topology"] = json!({
        "registry_path": [{
            "logical_name_id": logical_name_id,
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "namehash:alice.base.eth",
            "resource_id": resource_id.to_string(),
            "binding_kind": "declared_registry_path"
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
            "latest_event_kind": "ResolverChanged"
        }],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": worker_row.record_version_boundary.clone(),
            "record_version_boundary": worker_row.record_version_boundary.clone()
        },
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "latest_event_kind": null
        }
    });
    database.insert_name_current_row(name_row.clone()).await?;

    let requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.request_key = request_key.clone();
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions.clone(),
    });
    trace.manifest_context = json!({
        "manifest_versions": [{
            "source_family": "basenames_execution",
            "manifest_version": 2
        }]
    });
    trace.contracts_called = json!([
        {
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "selector": "0x9061b923"
        }
    ]);
    trace.gateway_digests = json!(["sha256:ccip-request", "sha256:ccip-response"]);
    trace.request_metadata = json!({
        "surface": "alice.base.eth",
        "record_keys": ["text:com.twitter", "addr:60"],
        "entrypoint": "l1_resolver",
        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "latest_event_kind": null
        }
    });
    trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_declared_topology".to_owned(),
            input_digest: Some("sha256:topology-input".to_owned()),
            output_digest: Some("sha256:topology-output".to_owned()),
            latency_ms: Some(4),
            canonicality_dependency: json!({
                "base-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "entrypoint": "l1_resolver",
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "call_l1_resolver".to_owned(),
            input_digest: Some("sha256:l1-input".to_owned()),
            output_digest: Some("sha256:l1-output".to_owned()),
            latency_ms: Some(17),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "name": "alice.base.eth",
                "record_count": 2
            }),
        },
        ExecutionTraceStep {
            step_index: 2,
            step_kind: "ccip_offchain_lookup".to_owned(),
            input_digest: Some("sha256:ccip-input".to_owned()),
            output_digest: Some("sha256:ccip-output".to_owned()),
            latency_ms: Some(29),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "gateway_digest": "sha256:ccip-request"
            }),
        },
        ExecutionTraceStep {
            step_index: 3,
            step_kind: "resolve_with_proof".to_owned(),
            input_digest: Some("sha256:proof-input".to_owned()),
            output_digest: Some("sha256:proof-output".to_owned()),
            latency_ms: Some(11),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "proof_kind": "signature"
            }),
        },
    ];

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        worker_row.record_version_boundary.clone(),
        worker_row.record_version_boundary.clone(),
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.requested_chain_positions = requested_chain_positions;
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-ethereum basenames mixed resolution request failed")?;
    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-ethereum basenames execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(explain_response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "conflict");

    let explain_payload: ErrorResponse = read_json(explain_response).await?;
    assert_eq!(explain_payload.error.code, "not_found");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_keeps_basenames_transport_explicit_without_projected_topology() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6405);
    let token_lineage_id = Uuid::from_u128(0x6505);
    let surface_binding_id = Uuid::from_u128(0x6605);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000036);
    let request_key = basenames_resolution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record",
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames declared resolution request failed before missing-topology assertions")?;
    assert_eq!(declared_response.status(), StatusCode::OK);
    let declared_payload: ResolutionResponse = read_json(declared_response).await?;
    let record_inventory_boundary = declared_payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("record_inventory"))
        .and_then(|value| value.get("record_version_boundary"))
        .cloned()
        .context("basenames declared resolution must expose record_inventory boundary")?;
    let mut name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("basenames missing-topology test requires name_current row")?;
    append_basenames_execution_manifest_version(&mut name_row);
    insert_basenames_supported_ethereum_position(&mut name_row);
    database.insert_name_current_row(name_row.clone()).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_inventory_boundary,
    )
    .await?
    .context(
        "worker-produced basenames record_inventory_current row must exist after transport seed",
    )?;
    let topology = basenames_supported_topology(
        logical_name_id,
        resource_id,
        &worker_row.record_version_boundary,
    );

    let requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.namespace = "basenames".to_owned();
    trace.request_key = request_key.clone();
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions.clone(),
    });
    trace.manifest_context = json!({
        "manifest_versions": [{
            "source_family": "basenames_execution",
            "manifest_version": 2
        }]
    });
    trace.contracts_called = json!([
        {
            "chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "selector": "0x9061b923"
        }
    ]);
    trace.gateway_digests = json!(["sha256:ccip-request", "sha256:ccip-response"]);
    trace.request_metadata = json!({
        "surface": "alice.base.eth",
        "record_keys": ["text:com.twitter", "addr:60"],
        "entrypoint": "l1_resolver",
        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "latest_event_kind": null
        }
    });
    trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_declared_topology".to_owned(),
            input_digest: Some("sha256:topology-input".to_owned()),
            output_digest: Some("sha256:topology-output".to_owned()),
            latency_ms: Some(4),
            canonicality_dependency: json!({
                "base-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "entrypoint": "l1_resolver",
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "call_l1_resolver".to_owned(),
            input_digest: Some("sha256:l1-input".to_owned()),
            output_digest: Some("sha256:l1-output".to_owned()),
            latency_ms: Some(17),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "name": "alice.base.eth",
                "record_count": 2
            }),
        },
        ExecutionTraceStep {
            step_index: 2,
            step_kind: "ccip_offchain_lookup".to_owned(),
            input_digest: Some("sha256:ccip-input".to_owned()),
            output_digest: Some("sha256:ccip-output".to_owned()),
            latency_ms: Some(29),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "gateway_digest": "sha256:ccip-request"
            }),
        },
        ExecutionTraceStep {
            step_index: 3,
            step_kind: "resolve_with_proof".to_owned(),
            input_digest: Some("sha256:proof-input".to_owned()),
            output_digest: Some("sha256:proof-output".to_owned()),
            latency_ms: Some(11),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_hash": "0xbase-binding",
                    "block_number": 100,
                    "state": "finalized"
                }
            }),
            step_payload: json!({
                "proof_kind": "signature"
            }),
        },
    ];

    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        worker_row.record_version_boundary.clone(),
        worker_row.record_version_boundary.clone(),
    );
    outcome.namespace = "basenames".to_owned();
    outcome.cache_key.requested_chain_positions = requested_chain_positions;
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let mut profile_outcome = outcome.clone();
    profile_outcome.cache_key.request_key = basenames_resolution_request_key(&["addr:60"]);
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    upsert_execution_outcome(&database.pool, &profile_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-topology basenames mixed resolution request failed")?;
    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("missing-topology basenames execution explain request failed")?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let payload: Value = read_json(response).await?;
        anyhow::bail!("expected basenames profile response 200, got {status}: {payload}");
    }
    assert_eq!(explain_response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .context("missing-topology basenames mixed resolution must include declared_state")?;

    assert_eq!(declared_state.get("topology"), Some(&topology));
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
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
                },
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported"
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
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "basenames_execution",
                    "role": "l1_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"
                },
                "resolver_discovery_path": topology.get("resolver_path").cloned().expect("topology must include resolver_path"),
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "base-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_l1_resolver",
                        "input_digest": "sha256:l1-input",
                        "output_digest": "sha256:l1-output",
                        "latency": 17,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 2,
                        "step_kind": "ccip_offchain_lookup",
                        "input_digest": "sha256:ccip-input",
                        "output_digest": "sha256:ccip-output",
                        "latency": 29,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 3,
                        "step_kind": "resolve_with_proof",
                        "input_digest": "sha256:proof-input",
                        "output_digest": "sha256:proof-output",
                        "latency": 11,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": persisted_verified_queries
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_keeps_out_of_class_basenames_transport_explicit() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6400);
    let token_lineage_id = Uuid::from_u128(0x6500);
    let surface_binding_id = Uuid::from_u128(0x6600);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
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
        .context("basenames declared resolution must expose record_inventory boundary")?;
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
            "binding_kind": "declared_registry_path"
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
            "latest_event_kind": "ResolverChanged"
        }],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": worker_row.record_version_boundary.clone(),
            "record_version_boundary": worker_row.record_version_boundary.clone()
        },
        "transport": {
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0x0000000000000000000000000000000000000bad",
            "latest_event_kind": null
        }
    });
    database.insert_name_current_row(name_row).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("out-of-class basenames mixed resolution request failed")?;
    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("out-of-class basenames execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(explain_response.status(), StatusCode::NOT_FOUND);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(resolution_unsupported_verified_state(&["addr:60", "text"]))
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
async fn get_resolution_keeps_basenames_deferred_path_classes_selector_local() -> Result<()> {
    for case in BasenamesDeferredVerifiedPathCase::all() {
        assert_basenames_deferred_verified_path_case_stays_selector_local(case).await?;
    }

    Ok(())
}

#[tokio::test]
async fn get_resolution_ensv2_sepolia_dev_verified_and_explain_stay_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let cases = [
        ("alice.eth", "Alice.eth", true, 0x8d10_u128),
        ("bob.alice.eth", "Bob.alice.eth", false, 0x8d20_u128),
    ];

    for (normalized_name, canonical_display_name, include_projected_topology, base_id) in cases {
        let logical_name_id = format!("ens:{normalized_name}");
        let resource_id = Uuid::from_u128(base_id);
        let token_lineage_id = Uuid::from_u128(base_id + 1);
        let surface_binding_id = Uuid::from_u128(base_id + 2);
        let execution_trace_id =
            Uuid::from_u128(0x0e7ec7ace00000000000000000008d10 + base_id);
        let row = ensv2_sepolia_resolution_row(
            &logical_name_id,
            normalized_name,
            canonical_display_name,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            include_projected_topology,
        );
        let sepolia_boundary = ensv2_sepolia_resolution_boundary(&logical_name_id, resource_id);
        let requested_chain_positions =
            requested_chain_positions_from_name_current(&row.chain_positions);
        let chain_positions_query =
            encode_query_value(&serde_json::to_string(&row.chain_positions)?);
        let request_key = bigname_storage::normalized_resolution_request_key_from_record_keys(
            "ens",
            normalized_name,
            &["addr:60".to_owned()],
        );
        let persisted_verified_queries = json!([
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]);

        database
            .seed_name_current_binding_migrated(
                &logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
        database.insert_name_current_row(row.clone()).await?;

        let mut trace = resolution_execution_trace(
            execution_trace_id,
            &request_key,
            &["addr:60"],
            persisted_verified_queries.clone(),
        );
        trace.chain_context = json!({
            "requested_positions": requested_chain_positions.clone(),
        });
        trace.manifest_context = json!({
            "manifest_versions": row.provenance["manifest_versions"].clone(),
        });
        trace.contracts_called = json!([{
            "chain_id": "ethereum-sepolia",
            "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
            "selector": "0x9061b923",
        }]);
        let mut outcome = resolution_execution_outcome_with_boundaries(
            execution_trace_id,
            &request_key,
            persisted_verified_queries,
            sepolia_boundary.clone(),
            sepolia_boundary,
        );
        outcome.cache_key.requested_chain_positions = requested_chain_positions;
        outcome.cache_key.manifest_versions = row.provenance["manifest_versions"].clone();
        upsert_execution_trace(&database.pool, &trace).await?;
        upsert_execution_outcome(&database.pool, &outcome).await?;

        let verified_response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/profiles/names/{normalized_name}?mode=verified&chain_positions={chain_positions_query}&meta=full"
                    ))
                    .body(Body::empty())
                    .expect("ENSv2 Sepolia verified request must build"),
            )
            .await
            .with_context(|| {
                format!("ENSv2 Sepolia verified request failed for {normalized_name}")
            })?;
        let explain_response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/explain/resolutions/ens/{normalized_name}/execution?records=addr:60"
                    ))
                    .body(Body::empty())
                    .expect("ENSv2 Sepolia explain request must build"),
            )
            .await
            .with_context(|| {
                format!("ENSv2 Sepolia explain request failed for {normalized_name}")
            })?;

        assert_eq!(
            verified_response.status(),
            StatusCode::OK,
            "{normalized_name}"
        );
        assert_eq!(
            explain_response.status(),
            StatusCode::NOT_FOUND,
            "{normalized_name}"
        );

        let verified_payload: ResolutionResponse = read_json(verified_response).await?;
        let explain_payload: ErrorResponse = read_json(explain_response).await?;
        assert_eq!(
            verified_payload.verified_state,
            Some(json!({ "verified_queries": [] })),
            "{normalized_name}"
        );
        assert_eq!(
            verified_payload.provenance.get("execution_trace_id"),
            Some(&Value::Null),
            "{normalized_name}"
        );
        assert_eq!(explain_payload.error.code, "not_found", "{normalized_name}");
        assert_eq!(
            explain_payload.error.message,
            format!(
                "persisted resolution execution explain was not found for name {normalized_name} in namespace ens"
            ),
            "{normalized_name}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_ensv2_sepolia_dev_declared_record_sections_stay_unsupported_even_with_projection_row()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let normalized_name = "alice.eth";
    let canonical_display_name = "Alice.eth";
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x8d40);
    let token_lineage_id = Uuid::from_u128(0x8d41);
    let surface_binding_id = Uuid::from_u128(0x8d42);
    let row = ensv2_sepolia_resolution_row(
        logical_name_id,
        normalized_name,
        canonical_display_name,
        surface_binding_id,
        resource_id,
        token_lineage_id,
        true,
    );
    let chain_positions_query = encode_query_value(&serde_json::to_string(&row.chain_positions)?);
    let expected_topology = row
        .declared_summary
        .get("topology")
        .cloned()
        .context("ENSv2 Sepolia fixture must include projected topology")?;

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(ensv2_sepolia_record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/profiles/names/{normalized_name}?mode=declared&chain_positions={chain_positions_query}&meta=full"
                ))
                .body(Body::empty())
                .expect("ENSv2 Sepolia declared request must build"),
        )
        .await
        .context("ENSv2 Sepolia declared resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(declared_state.get("topology"), Some(&expected_topology));
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record inventory is not yet projected",
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record cache is not yet projected",
        }))
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_uses_supported_persisted_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000022);
    let request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "avatar",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash"
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        STANDARD_PROFILE_RECORD_KEYS,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": persisted_verified_queries
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_modes_return_stale_when_persisted_outcome_omits_supported_selector()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);
    let request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        STANDARD_PROFILE_RECORD_KEYS,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    for mode in ["verified", "both"] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/profiles/names/alice.eth?mode={mode}&meta=full"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("{mode} partial verified resolution request failed"))?;

        assert_eq!(response.status(), StatusCode::CONFLICT, "{mode}");

        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "stale", "{mode}");
        assert_eq!(
            payload.error.message,
            "persisted verified resolution output is not available for the selected snapshot",
            "{mode}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_reads_avatar_only_persisted_answer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002b);
    let request_key = resolution_execution_request_key(&["avatar"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-avatar-only.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    for mode in ["verified", "both"] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/profiles/names/alice.eth?mode={mode}&meta=full"
                    ))
                    .body(Body::empty())
                    .expect("avatar-only request must build"),
            )
            .await
            .with_context(|| format!("{mode} avatar-only resolution request failed"))?;

        assert_eq!(response.status(), StatusCode::CONFLICT, "{mode}");

        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "stale", "{mode}");
        assert_eq!(
            payload.error.message,
            "verified resolution RPC provider for ethereum-mainnet is not configured; set BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>",
            "{mode}"
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_modes_keep_missing_avatar_output_stale_for_selected_snapshot()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002c);
    let request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        STANDARD_PROFILE_RECORD_KEYS,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    for records in ["avatar", "avatar,addr:60"] {
        for mode in ["verified", "both"] {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/profiles/names/alice.eth?mode={mode}&meta=full"
                        ))
                        .body(Body::empty())
                        .expect("missing-avatar request must build"),
                )
                .await
                .with_context(|| {
                    format!("{mode} missing-avatar resolution request failed for {records}")
                })?;

            assert_eq!(response.status(), StatusCode::CONFLICT, "{mode} {records}");

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "stale", "{mode} {records}");
            assert_eq!(
                payload.error.message,
                "persisted verified resolution output is not available for the selected snapshot",
                "{mode} {records}"
            );
        }
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
    let request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "not_found",
            "failure_reason": "no_addr_record"
        },
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash"
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        STANDARD_PROFILE_RECORD_KEYS,
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution alias request failed")?;

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
        "record inventory should still load through the persisted readback lane"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({ "verified_queries": persisted_verified_queries }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_surfaces_persisted_avatar_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000023);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("verified request must build"),
        )
        .await
        .context("verified resolution request with contenthash failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("mixed request must build"),
        )
        .await
        .context("mixed resolution request with contenthash failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_when_persisted_answer_is_missing()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
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
async fn get_resolution_execution_explain_surfaces_persisted_avatar_answers_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000024);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("explain request must build"),
        )
        .await
        .context("resolution execution explain request with contenthash failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("resolution request must build"),
        )
        .await
        .context("resolution request with contenthash failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, "finalized");
    assert_eq!(resolution_payload.consistency, "head");
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "not_found",
                    "failure_reason": "no_text_record"
                },
                {
                    "record_key": "contenthash",
                    "status": "success",
                    "value": {
                        "value": contenthash
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_rejects_duplicate_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("duplicate resolution execution explain request failed")?;

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
async fn get_resolution_execution_explain_rejects_malformed_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=:avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed resolution execution explain request failed")?;

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
async fn get_resolution_mode_parsing_populates_expected_sections() -> Result<()> {
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

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default resolution request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
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

    assert!(default_payload.declared_state.is_some());
    assert_eq!(
        default_payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );
    assert!(declared_payload.declared_state.is_some());
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(
        both_payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_modes_keep_missing_supported_output_stale_for_selected_snapshot()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    let chain_positions_query = encode_query_value(&serde_json::to_string(&row.chain_positions)?);

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
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let selector_cases = vec![
        ("default selector", String::new()),
        (
            "explicit chain_positions",
            format!("&chain_positions={chain_positions_query}"),
        ),
        ("explicit at", "&at=2026-04-17T00%3A00%3A03Z".to_owned()),
        ("consistency floor", "&consistency=finalized".to_owned()),
    ];
    for mode in ["verified", "both"] {
        for (label, selector_query) in &selector_cases {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/profiles/names/alice.eth?mode={mode}{selector_query}&meta=full"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!("{mode} resolution request with {label} failed")
                })?;

            assert_eq!(response.status(), StatusCode::CONFLICT, "{mode} {label}");

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "stale", "{mode} {label}");
            assert_eq!(
                payload.error.message,
                "verified resolution RPC provider for ethereum-mainnet is not configured; set BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>",
                "{mode} {label}"
            );
        }
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_supports_projected_wildcard_topology() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let wildcard_source_resource_id = Uuid::from_u128(0x4400);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000027);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    seed_supported_wildcard_rebuild_inputs(
        &database,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        wildcard_source_resource_id,
    )
    .await?;
    let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
        .await?
        .context("wildcard execution explain test requires rebuilt name_current row")?;
    let projected_topology = projected_resolution_topology(&name_row)?;
    let wildcard_source = projected_topology
        .pointer("/wildcard/source")
        .cloned()
        .context("wildcard projected topology must include source")?;
    let wildcard_labels = projected_topology
        .pointer("/wildcard/matched_labels")
        .cloned()
        .context("wildcard projected topology must include matched_labels")?;
    let (topology_boundary, record_boundary) = projected_resolution_boundaries(&name_row)?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ["addr:60"],
        "entrypoint": "universal_resolver",
        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
        "wildcard": {
            "source": wildcard_source.clone(),
            "matched_labels": wildcard_labels.clone()
        }
    });
    trace.chain_context = json!({
        "requested_positions": requested_chain_positions_from_name_current(&name_row.chain_positions),
    });
    let mut outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        topology_boundary.clone(),
        record_boundary.clone(),
    );
    outcome.cache_key.requested_chain_positions =
        requested_chain_positions_from_name_current(&name_row.chain_positions);
    outcome.cache_key.manifest_versions = name_row
        .provenance
        .get("manifest_versions")
        .cloned()
        .unwrap_or_else(|| json!([]));
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("wildcard resolution execution explain request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=verified&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("wildcard mixed resolution request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;

    assert_eq!(
        resolution_payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("execution"))
            .and_then(|execution| execution.get("resolver_discovery_path")),
        projected_topology.get("resolver_path")
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("execution"))
            .and_then(|execution| execution.get("wildcard")),
        Some(&json!({
            "source": wildcard_source,
            "matched_labels": wildcard_labels
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_preserves_projected_topology_for_deferred_ancestor_selected_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let ancestor_resource_id = Uuid::from_u128(0x5500);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000028);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let ancestor_boundary = record_inventory_boundary("ens:eth", ancestor_resource_id);
    let projected_topology = json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "declared_registry_path"
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": "ens:eth",
                "namespace": "ens",
                "normalized_name": "eth",
                "canonical_display_name": "Eth",
                "resource_id": ancestor_resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000def",
                "latest_event_kind": "ResolverChanged"
            }
        ],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": ancestor_boundary.clone(),
            "record_version_boundary": ancestor_boundary.clone()
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    });
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@ancestor"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": projected_topology.clone()
    });
    database.insert_name_current_row(row).await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        ancestor_boundary.clone(),
        ancestor_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred ancestor-selected mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&projected_topology)
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_preserves_projected_transport_for_deferred_transport_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000029);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let route_boundary = record_inventory_boundary(logical_name_id, resource_id);
    let projected_topology = json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "declared_registry_path"
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "resource_id": resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        ],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": route_boundary.clone(),
            "record_version_boundary": route_boundary.clone()
        },
        "transport": {
            "source_chain_id": "ethereum-mainnet",
            "target_chain_id": "base-mainnet",
            "contract_address": "0x000000000000000000000000000000000000beef",
            "latest_event_kind": "TransportResolved"
        }
    });
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": projected_topology.clone()
    });
    database.insert_name_current_row(row).await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        route_boundary.clone(),
        route_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("transport-assisted mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&projected_topology)
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({ "verified_queries": [] }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_for_deferred_ancestor_selected_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let ancestor_resource_id = Uuid::from_u128(0x5500);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let ancestor_boundary = record_inventory_boundary("ens:eth", ancestor_resource_id);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": {
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [
                {
                    "logical_name_id": "ens:eth",
                    "namespace": "ens",
                    "normalized_name": "eth",
                    "canonical_display_name": "Eth",
                    "resource_id": ancestor_resource_id.to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000def",
                    "latest_event_kind": "ResolverChanged"
                }
            ],
            "wildcard": {
                "source": null,
                "matched_labels": []
            },
            "alias": {
                "final_target": null,
                "hops": []
            },
            "version_boundaries": {
                "topology_version_boundary": ancestor_boundary.clone(),
                "record_version_boundary": ancestor_boundary.clone()
            },
            "transport": {
                "source_chain_id": null,
                "target_chain_id": null,
                "contract_address": null,
                "latest_event_kind": null
            }
        }
    });
    database.insert_name_current_row(row).await?;

    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@ancestor"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        ancestor_boundary.clone(),
        ancestor_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred ancestor-selected resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_for_deferred_transport_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002b);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let route_boundary = record_inventory_boundary(logical_name_id, resource_id);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": {
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [
                {
                    "logical_name_id": logical_name_id,
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "resource_id": resource_id.to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged"
                }
            ],
            "wildcard": {
                "source": null,
                "matched_labels": []
            },
            "alias": {
                "final_target": null,
                "hops": []
            },
            "version_boundaries": {
                "topology_version_boundary": route_boundary.clone(),
                "record_version_boundary": route_boundary.clone()
            },
            "transport": {
                "source_chain_id": "ethereum-mainnet",
                "target_chain_id": "base-mainnet",
                "contract_address": "0x000000000000000000000000000000000000beef",
                "latest_event_kind": "TransportResolved"
            }
        }
    });
    database.insert_name_current_row(row).await?;

    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        route_boundary.clone(),
        route_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred transport resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );

    database.cleanup().await?;
    Ok(())
}
#[tokio::test]
async fn get_resolution_profile_rejects_records_query() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("profile records-query rejection request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records is not supported on /v1/profiles/names/{name}; use /v1/names/{namespace}/{name}/records for selector-specific reads"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_rejects_invalid_snapshot_selectors_as_invalid_input() -> Result<()> {
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

    for (label, query, expected_message_fragment) in cases {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/profiles/names/alice.eth?mode=declared&{query}&meta=full"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| format!("{label} resolution selector request failed"))?;

        assert_public_invalid_input_response(response, expected_message_fragment).await?;
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_not_found_when_exact_surface_projection_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request without exact surface projection failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name alice.eth was not found in namespace ens"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_supported_topology_for_direct_ens_binding() -> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);

    {
        let declared_state = payload
            .declared_state
            .as_ref()
            .expect("declared_state must be present");
        let topology_record_boundary = declared_state
            .pointer("/topology/version_boundaries/record_version_boundary")
            .expect("topology record boundary must be present");
        let record_inventory = declared_state
            .get("record_inventory")
            .expect("record_inventory must be present");
        let record_cache = declared_state
            .get("record_cache")
            .expect("record_cache must be present");

        assert_eq!(
            record_inventory.get("record_version_boundary"),
            Some(topology_record_boundary)
        );
        assert_eq!(
            record_cache.get("record_version_boundary"),
            Some(topology_record_boundary)
        );
        assert_eq!(
            record_keys(record_cache.get("entries").expect("entries must be present")),
            ["addr:60", "avatar", "text:com.twitter", "contenthash"]
        );
        assert_eq!(
            record_keys(
                record_inventory
                    .get("selectors")
                    .expect("selectors must be present")
            ),
            ["addr:60", "avatar", "text:com.twitter"]
        );
        assert_eq!(
            record_keys(
                record_cache
                    .get("entries")
                    .expect("cache entries must be present")
            ),
            ["addr:60", "avatar", "text:com.twitter", "contenthash"]
        );
        assert_eq!(
            record_statuses(
                record_cache
                    .get("entries")
                    .expect("cache entries must be present")
            ),
            ["success", "unsupported", "not_found", "not_found"]
        );
    }

    assert_eq!(
        payload.declared_state,
        Some(json!({
            "topology": {
                "registry_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "binding_kind": "declared_registry_path",
                    }
                ],
                "subregistry_path": [],
                "resolver_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
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
                    "topology_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                    "record_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                },
                "transport": {
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                },
            },
            "record_inventory": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "enumeration_basis": {
                    "observed_selectors": true,
                    "capability_declared_families": true,
                    "globally_enumerable": false,
                },
                "selectors": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "cacheable": true,
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "cacheable": true,
                    },
                    {
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "cacheable": false,
                    }
                ],
                "explicit_gaps": [
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "gap_reason": "not_observed_on_current_resolver",
                    }
                ],
                "unsupported_families": [
                    {
                        "record_family": "abi",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_family": "pubkey",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ],
                "last_change": {
                    "normalized_event_id": 1200,
                    "event_kind": "RecordsChanged",
                    "chain_position": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_003,
                        "block_hash": "0xlastchange",
                        "timestamp": "2026-04-17T00:00:04Z",
                    }
                }
            },
            "record_cache": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "entries": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x0000000000000000000000000000000000000abc",
                        }
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "status": "not_found",
                    },
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    }
                ]
            }
        }))
    );

    let compact_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("compact resolution request failed")?;

    assert_eq!(compact_response.status(), StatusCode::OK);

    let compact_payload: ResolutionResponse = read_json(compact_response).await?;
    assert_eq!(
        compact_payload.data,
        json!({
            "name": "alice.eth",
            "namespace": "ens",
            "namehash": "namehash:alice.eth",
            "resource_id": resource_id.to_string(),
        })
    );
    assert!(
        compact_payload.data.get("normalized_name").is_none(),
        "compact profile data omits routine normalized_name"
    );
    assert!(
        compact_payload.data.get("logical_name_id").is_none(),
        "compact profile data omits internal logical_name_id"
    );
    assert_eq!(compact_payload.provenance, Value::Null);
    assert_eq!(compact_payload.coverage, Value::Null);
    assert_eq!(compact_payload.chain_positions, Value::Null);
    assert!(compact_payload.consistency.is_empty());
    assert!(compact_payload.last_updated.is_empty());
    assert_eq!(compact_payload.verified_state, None);
    let compact_declared_state = compact_payload
        .declared_state
        .as_ref()
        .expect("compact declared_state must be present");
    assert_eq!(
        compact_declared_state.pointer("/topology/resolver_path/0/address"),
        Some(&json!("0x0000000000000000000000000000000000000abc"))
    );
    assert!(
        compact_declared_state
            .pointer("/topology/version_boundaries")
            .is_none(),
        "compact profile topology omits diagnostic boundaries"
    );
    assert_eq!(
        compact_declared_state
            .pointer("/record_inventory/explicit_gaps")
            .cloned(),
        Some(json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver",
            }
        ]))
    );
    assert!(
        compact_declared_state
            .pointer("/record_inventory/record_version_boundary")
            .is_none(),
        "compact profile inventory omits diagnostic boundary"
    );
    assert!(
        compact_declared_state
            .pointer("/record_cache/record_version_boundary")
            .is_none(),
        "compact profile cache omits diagnostic boundary"
    );
    assert_eq!(
        record_keys(
            compact_declared_state
                .pointer("/record_cache/entries")
                .expect("compact cache entries must be present")
        ),
        ["addr:60", "avatar", "text:com.twitter", "contenthash"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    let topology = declared_state
        .get("topology")
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let version_boundaries = topology
        .get("version_boundaries")
        .and_then(Value::as_object)
        .expect("version_boundaries must be present");

    assert_eq!(
        version_boundaries.get("topology_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        version_boundaries.get("record_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_cache")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_record_inventory_sections_when_projection_row_is_missing()
-> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record inventory is not yet projected",
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record cache is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_publicresolver_profile_reads_supported_ensv1_records()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x9d40);
    let token_lineage_id = Uuid::from_u128(0x9d41);
    let surface_binding_id = Uuid::from_u128(0x9d42);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000009d40);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d41";
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-dynamic.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-dynamic"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000dd"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let profile_request_key = resolution_execution_request_key(STANDARD_PROFILE_CACHE_RECORD_KEYS);
    let profile_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000dd"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-dynamic.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-dynamic"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "contenthash",
            "status": "not_found",
            "failure_reason": "no_contenthash"
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.steps[0].step_payload = json!({
        "entrypoint": "universal_resolver",
        "resolver": dynamic_resolver_address,
    });
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    let profile_outcome = resolution_execution_outcome(
        execution_trace_id,
        &profile_request_key,
        profile_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;
    upsert_execution_outcome(&database.pool, &profile_outcome).await?;

    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("resolution request must build"),
        )
        .await
        .context("supported dynamic resolver resolution request failed")?;
    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("explain request must build"),
        )
        .await
        .context("supported dynamic resolver execution explain request failed")?;

    assert_eq!(resolution_response.status(), StatusCode::OK);
    assert_eq!(explain_response.status(), StatusCode::OK);

    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let declared_state = resolution_payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/address"),
        Some(&json!(dynamic_resolver_address))
    );
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/chain_id"),
        Some(&json!("ethereum-mainnet"))
    );
    assert_eq!(
        declared_state.pointer("/record_inventory/selectors"),
        Some(&json!([
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
        ]))
    );
    assert_eq!(
        resolution_payload.verified_state,
        Some(json!({
            "verified_queries": profile_verified_queries
        }))
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.pointer("/execution/resolver_discovery_path/0/address")),
        Some(&json!(dynamic_resolver_address))
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.pointer("/verified_queries")),
        Some(&json!([
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice-dynamic.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice-dynamic"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000dd"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_profile_non_graduation_keeps_ensv1_records_explicit()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x9d10);
    let token_lineage_id = Uuid::from_u128(0x9d11);
    let surface_binding_id = Uuid::from_u128(0x9d12);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d11";

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
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(
            dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            ),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ENSv1 dynamic resolver non-graduation request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/address"),
        Some(&json!(dynamic_resolver_address))
    );
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/chain_id"),
        Some(&json!("ethereum-mainnet"))
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|inventory| inventory.get("explicit_gaps")),
        Some(&json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver",
            }
        ]))
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|inventory| inventory.get("unsupported_families")),
        Some(&json!([
            {
                "record_family": "addr",
                "unsupported_reason": "resolver_family_pending",
            },
            {
                "record_family": "text",
                "unsupported_reason": "resolver_family_pending",
            }
        ]))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ]
        }))
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_pending_profile_returns_observed_addr_with_text_pending()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x9d13);
    let token_lineage_id = Uuid::from_u128(0x9d14);
    let surface_binding_id = Uuid::from_u128(0x9d15);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d13";

    let mut inventory_row = dynamic_resolver_unsupported_profile_record_inventory_current_row(
        logical_name_id,
        resource_id,
    );
    let record_version_boundary = inventory_row.record_version_boundary.clone();
    inventory_row.enumeration_basis = json!({
        "observed_selectors": true,
        "capability_declared_families": true,
        "globally_enumerable": false
    });
    inventory_row.selectors = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        }
    ]);
    inventory_row.explicit_gaps = json!([]);
    inventory_row.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000a1"
            }
        }
    ]);
    inventory_row.last_change = Some(json!({
        "normalized_event_id": 1202,
        "event_kind": "RecordChanged",
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xobservedaddr",
            "timestamp": "2026-04-17T00:00:05Z"
        }
    }));

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
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(inventory_row)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pending dynamic resolver observed addr request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/record_inventory/selectors"),
        Some(&json!([
            {
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }
        ]))
    );
    assert_eq!(
        declared_state.pointer("/record_inventory/explicit_gaps"),
        Some(&json!([]))
    );
    assert_eq!(
        declared_state.pointer("/record_inventory/unsupported_families"),
        Some(&json!([
            {
                "record_family": "addr",
                "unsupported_reason": "resolver_family_pending",
            },
            {
                "record_family": "text",
                "unsupported_reason": "resolver_family_pending",
            }
        ]))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_version_boundary,
            "entries": [
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000a1",
                    }
                }
            ]
        }))
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_pending_profile_keeps_missing_verified_output_stale()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x9d50);
    let token_lineage_id = Uuid::from_u128(0x9d51);
    let surface_binding_id = Uuid::from_u128(0x9d52);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d51";

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
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(
            dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            ),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pending dynamic resolver mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::CONFLICT);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "stale");
    assert_eq!(
        payload.error.message,
        "verified resolution RPC provider for ethereum-mainnet is not configured; set BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_pending_profile_reads_persisted_verified_output()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x9d53);
    let token_lineage_id = Uuid::from_u128(0x9d54);
    let surface_binding_id = Uuid::from_u128(0x9d55);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000045);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000d51";
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000dd"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

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
        .insert_name_current_row(name_current_row_with_current_resolver(
            exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            ),
            "ethereum-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    let mut inventory_row =
        dynamic_resolver_unsupported_profile_record_inventory_current_row(
            logical_name_id,
            resource_id,
        );
    inventory_row.selectors = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "cacheable": true
        }
    ]);
    inventory_row.explicit_gaps = json!([]);
    inventory_row.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "unsupported",
            "unsupported_reason": "resolver_family_pending"
        }
    ]);
    database
        .insert_record_inventory_current_row(inventory_row)
        .await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.steps[0].step_payload = json!({
        "entrypoint": "universal_resolver",
        "resolver": dynamic_resolver_address,
    });
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries.clone(),
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=both&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("pending dynamic resolver persisted verified request failed")?;
    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("explain request must build"),
        )
        .await
        .context("pending dynamic resolver persisted explain request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(explain_response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/record_cache/entries/0/unsupported_reason"),
        Some(&json!("resolver_family_pending"))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({ "verified_queries": persisted_verified_queries }))
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("verified_queries")),
        payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("verified_queries"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_l2resolver_profile_reads_supported_basenames_records()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let namespace = "basenames";
    let normalized_name = "alice.base.eth";
    let canonical_display_name = "Alice.base.eth";
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9d60);
    let token_lineage_id = Uuid::from_u128(0x9d61);
    let surface_binding_id = Uuid::from_u128(0x9d62);
    let dynamic_resolver_address = "0x0000000000000000000000000000000000000b60";
    let inventory_row =
        basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
    let record_version_boundary = inventory_row.record_version_boundary.clone();

    database
        .seed_name_current_binding(
            logical_name_id,
            namespace,
            normalized_name,
            canonical_display_name,
            "namehash:alice.base.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(name_current_row_with_current_resolver(
            resolution_route_name_row(
                namespace,
                normalized_name,
                canonical_display_name,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            ),
            "base-mainnet",
            dynamic_resolver_address,
        ))
        .await?;
    database
        .insert_record_inventory_current_row(inventory_row)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("supported Basenames dynamic resolver declared resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/address"),
        Some(&json!(dynamic_resolver_address))
    );
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/chain_id"),
        Some(&json!("base-mainnet"))
    );
    assert_eq!(
        declared_state.pointer("/topology/transport"),
        Some(&json!({
            "source_chain_id": "base-mainnet",
            "target_chain_id": "ethereum-mainnet",
            "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
            "latest_event_kind": null,
        }))
    );
    assert_eq!(
        declared_state.pointer("/topology/version_boundaries/record_version_boundary"),
        Some(&record_version_boundary)
    );
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "record_version_boundary": record_version_boundary.clone(),
            "enumeration_basis": {
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false,
            },
            "selectors": [
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "cacheable": true,
                }
            ],
            "explicit_gaps": [],
            "unsupported_families": [],
            "last_change": {
                "normalized_event_id": 1201,
                "event_kind": "RecordChanged",
                "chain_position": {
                    "chain_id": "base-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbase-binding",
                    "timestamp": "2026-04-17T00:00:03Z",
                }
            },
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_version_boundary,
            "entries": [
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                }
            ],
        }))
    );
    assert_eq!(payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_dynamic_resolver_profile_non_graduation_keeps_basenames_records_unsupported()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let namespace = "basenames";
    let cases = [
        (
            "pending.base.eth",
            "Pending.base.eth",
            "basenames:pending.base.eth",
            Uuid::from_u128(0x9d20),
            Uuid::from_u128(0x9d21),
            Uuid::from_u128(0x9d22),
            "0x0000000000000000000000000000000000000b11",
        ),
        (
            "unsupported.base.eth",
            "Unsupported.base.eth",
            "basenames:unsupported.base.eth",
            Uuid::from_u128(0x9d23),
            Uuid::from_u128(0x9d24),
            Uuid::from_u128(0x9d25),
            "0x0000000000000000000000000000000000000b12",
        ),
    ];

    for (
        normalized_name,
        canonical_display_name,
        logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
        dynamic_resolver_address,
    ) in cases
    {
        let inventory_row = basenames_dynamic_resolver_pending_record_inventory_current_row(
            logical_name_id,
            resource_id,
        );
        let record_version_boundary = inventory_row.record_version_boundary.clone();

        database
            .seed_name_current_binding(
                logical_name_id,
                namespace,
                normalized_name,
                canonical_display_name,
                &format!("namehash:{normalized_name}"),
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
        database
            .insert_name_current_row(name_current_row_with_current_resolver(
                resolution_route_name_row(
                    namespace,
                    normalized_name,
                    canonical_display_name,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                ),
                "base-mainnet",
                dynamic_resolver_address,
            ))
            .await?;
        database
            .insert_record_inventory_current_row(inventory_row)
            .await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/profiles/names/{normalized_name}?mode=declared&meta=full"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .with_context(|| {
                format!(
                    "Basenames dynamic resolver non-graduation request failed for {normalized_name}"
                )
            })?;

        assert_eq!(response.status(), StatusCode::OK);

        let payload: ResolutionResponse = read_json(response).await?;
        let declared_state = payload
            .declared_state
            .as_ref()
            .expect("declared_state must be present");
        assert_eq!(
            declared_state.pointer("/topology/resolver_path/0/address"),
            Some(&json!(dynamic_resolver_address))
        );
        assert_eq!(
            declared_state.pointer("/topology/resolver_path/0/chain_id"),
            Some(&json!("base-mainnet"))
        );
        assert_eq!(
            declared_state
                .get("record_inventory")
                .and_then(|inventory| inventory.get("selectors")),
            Some(&json!([]))
        );
        assert_eq!(
            declared_state
                .get("record_inventory")
                .and_then(|inventory| inventory.get("unsupported_families")),
            Some(&json!([
                {
                    "record_family": "addr",
                    "unsupported_reason": "resolver_family_pending",
                },
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending",
                }
            ]))
        );
        assert_eq!(
            declared_state.get("record_cache"),
            Some(&json!({
                "record_version_boundary": record_version_boundary,
                "entries": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "not_found",
                    },
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    },
                    {
                        "record_key": "text:description",
                        "record_family": "text",
                        "selector_key": "description",
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_key": "text:url",
                        "record_family": "text",
                        "selector_key": "url",
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_key": "text:email",
                        "record_family": "text",
                        "selector_key": "email",
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ]
            }))
        );
        assert_eq!(payload.verified_state, None);
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_narrow_record_cache_in_request_order() -> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with narrowed records failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    let record_inventory = declared_state
        .get("record_inventory")
        .expect("record_inventory must be present");
    let record_cache = declared_state
        .get("record_cache")
        .expect("record_cache must be present");

    assert_eq!(
        record_inventory.get("record_version_boundary"),
        record_cache.get("record_version_boundary")
    );
    assert_eq!(
        record_keys(
            record_cache
                .get("entries")
                .expect("cache entries must be present")
        ),
        ["addr:60", "avatar", "text:com.twitter", "contenthash"]
    );
    assert_eq!(
        record_statuses(
            record_cache
                .get("entries")
                .expect("cache entries must be present")
        ),
        ["success", "unsupported", "not_found", "not_found"]
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000abc",
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                },
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "status": "not_found",
                },
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_reuse_inventory_projection_for_later_checkpoint()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2201);
    let token_lineage_id = Uuid::from_u128(0x1101);
    let surface_binding_id = Uuid::from_u128(0x3301);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xbinding-later",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        }))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request at later checkpoint failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/record_inventory/selectors/0/record_key"),
        Some(&json!("addr:60"))
    );
    assert_eq!(
        declared_state.pointer("/record_cache/entries/0"),
        Some(&json!({
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000abc",
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[test]
fn get_resolution_declared_default_record_cache_keeps_missing_cacheable_selectors_explicit() {
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2210);
    let mut inventory_row = worker_record_inventory_current_row(logical_name_id, resource_id);
    let record_version_boundary = inventory_row.record_version_boundary.clone();
    inventory_row.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "unsupported",
            "unsupported_reason": "value_not_retained_in_normalized_events",
        }
    ]);
    assert_eq!(
        build_record_cache_section(Some(&inventory_row), &[], "unused"),
        json!({
            "record_version_boundary": record_version_boundary,
            "entries": [
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
                }
            ]
        })
    );
}

fn record_keys(records: &Value) -> Vec<&str> {
    records
        .as_array()
        .into_iter()
        .flatten()
        .map(|record| {
            record
                .get("record_key")
                .and_then(Value::as_str)
                .expect("record must include record_key")
        })
        .collect()
}

fn record_statuses(records: &Value) -> Vec<&str> {
    records
        .as_array()
        .into_iter()
        .flatten()
        .map(|record| {
            record
                .get("status")
                .and_then(Value::as_str)
                .expect("record must include status")
        })
        .collect()
}

#[tokio::test]
async fn get_resolution_declared_records_return_not_found_cache_entry_for_explicit_gap()
-> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with explicit-gap selector failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("explicit_gaps")),
        Some(&json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver",
            }
        ]))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000abc"
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending"
                },
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "status": "not_found"
                },
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_synthesize_unsupported_family_entries() -> Result<()> {
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
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with unsupported-family selectors failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_inventory"))
            .and_then(|inventory| inventory.get("unsupported_families")),
        Some(&json!([]))
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": worker_boundary,
            "entries": [
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
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_topology_for_non_direct_bindings() -> Result<()> {
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution topology is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_supported_topology_uses_terminal_null_hop_when_no_resolver_is_declared()
-> Result<()> {
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
            "chain_id": null,
            "address": null,
            "latest_event_kind": null
        }
    });
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(
            dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            ),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let topology = payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("topology"))
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let resolver_path = topology
        .get("resolver_path")
        .and_then(Value::as_array)
        .expect("resolver_path must be an array");
    assert_eq!(resolver_path.len(), 1);
    assert_eq!(
        resolver_path.first(),
        Some(&json!({
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "resource_id": resource_id.to_string(),
            "chain_id": null,
            "address": null,
            "latest_event_kind": null,
        }))
    );
    assert_eq!(
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("topology_version_boundary")),
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("record_version_boundary"))
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_inventory")),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "enumeration_basis": {
                "observed_selectors": false,
                "capability_declared_families": true,
                "globally_enumerable": false,
            },
            "selectors": [],
            "explicit_gaps": [],
            "unsupported_families": [],
            "last_change": null,
        }))
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ],
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_basenames_no_declared_resolver_addr60_stays_not_found_with_pending_inventory()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:no-resolver.base.eth";
    let normalized_name = "no-resolver.base.eth";
    let canonical_display_name = "No-resolver.base.eth";
    let resource_id = Uuid::from_u128(0x6260);
    let token_lineage_id = Uuid::from_u128(0x6261);
    let surface_binding_id = Uuid::from_u128(0x6262);
    let boundary = basenames_dynamic_resolver_record_inventory_boundary(
        logical_name_id,
        resource_id,
        None,
        None,
    );

    database
        .seed_name_current_binding(
            logical_name_id,
            "basenames",
            normalized_name,
            canonical_display_name,
            "namehash:no-resolver.base.eth",
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
    row.namespace = "basenames".to_owned();
    row.normalized_name = normalized_name.to_owned();
    row.canonical_display_name = canonical_display_name.to_owned();
    row.namehash = "namehash:no-resolver.base.eth".to_owned();
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": null,
            "address": null,
            "latest_event_kind": "ResolverChanged"
        },
        "topology": basenames_no_declared_resolver_topology(
            logical_name_id,
            normalized_name,
            canonical_display_name,
            resource_id,
            &boundary,
        )
    });
    row.provenance = json!({
        "normalized_event_ids": [1202],
        "raw_fact_refs": [
            {
                "kind": "log",
                "chain_id": "base-mainnet",
                "block_hash": "0xbase-binding"
            }
        ],
        "manifest_versions": [
            {
                "manifest_version": 6,
                "source_family": "basenames_base_registry",
                "chain": "base-mainnet",
                "deployment_epoch": "basenames_v1"
            }
        ],
        "execution_trace_id": null,
        "derivation_kind": "name_current_rebuild"
    });
    row.coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["basenames_base_registry"],
        "unsupported_reason": null,
        "enumeration_basis": "exact_name_profile"
    });
    row.chain_positions = json!({
        "base-mainnet": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "base-mainnet": "finalized"
        }
    });
    row.manifest_version = 6;
    database.insert_name_current_row(row).await?;

    let mut inventory_row = basenames_dynamic_resolver_pending_record_inventory_current_row(
        logical_name_id,
        resource_id,
    );
    inventory_row.record_version_boundary = boundary.clone();
    inventory_row.last_change = None;
    database
        .insert_record_inventory_current_row(inventory_row)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/no-resolver.base.eth?mode=declared&meta=full")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("Basenames no-declared-resolver request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/address"),
        Some(&Value::Null)
    );
    assert_eq!(
        declared_state.pointer("/topology/resolver_path/0/chain_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        declared_state
            .pointer("/record_cache/entries/0/record_key")
            .and_then(Value::as_str),
        Some("addr:60")
    );
    assert_eq!(
        declared_state
            .pointer("/record_cache/entries/0/status")
            .and_then(Value::as_str),
        Some("not_found")
    );
    assert_ne!(
        declared_state
            .pointer("/record_cache/entries/0/status")
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert!(
        declared_state
            .pointer("/record_cache/entries/0/unsupported_reason")
            .is_none()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_reuses_exact_name_envelope_fields() -> Result<()> {
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

    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/profiles/names/alice.eth?mode=declared")
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

    assert_eq!(
        resolution_payload.data,
        json!({
            "name": "alice.eth",
            "namespace": "ens",
            "namehash": "namehash:alice.eth",
            "resource_id": resource_id.to_string(),
        })
    );
    assert_ne!(resolution_payload.data, name_payload.data);
    assert_eq!(resolution_payload.provenance, Value::Null);
    assert_eq!(resolution_payload.coverage, Value::Null);
    assert_eq!(resolution_payload.chain_positions, Value::Null);
    assert!(resolution_payload.consistency.is_empty());
    assert!(resolution_payload.last_updated.is_empty());
    assert_eq!(resolution_payload.verified_state, None);

    database.cleanup().await?;
    Ok(())
}
