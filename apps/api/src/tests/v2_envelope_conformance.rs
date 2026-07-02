#[derive(Clone, Copy)]
struct V2ConformanceRoute {
    label: &'static str,
    error_uri: &'static str,
    success: V2SuccessFixture,
    envelope: V2TopLevelEnvelope,
    as_of: V2AsOfExpectation,
    tier: V2RouteTier,
    dictionary_allowlist: &'static [&'static str],
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum V2SuccessFixture {
    Lookup,
    Status,
    Name,
    NameRecords,
    Subnames,
    NameHistory,
    Permissions,
    AddressNames,
    PrimaryName,
    AddressHistory,
    Search,
    Events,
    Resolver,
    Namespace,
    DiagnosticsCoverage,
    DiagnosticsBinding,
    DiagnosticsAuthority,
    DiagnosticsRecords,
    DiagnosticsExecution,
    DiagnosticsNamespaceManifests,
    DiagnosticsEvents,
}

#[derive(Clone, Copy)]
enum V2TopLevelEnvelope {
    DataMeta,
    DataPageMeta,
}

#[derive(Clone, Copy)]
enum V2AsOfExpectation {
    Present,
    Absent,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum V2RouteTier {
    Product,
    Diagnostics,
}

// ADR 0006 section "Naming dictionary" Replaces (v1) plus "Deleted wire surface".
// Matching is by underscore-delimited term, with an optional trailing `s`, so
// storage-origin compounds and plural lists such as predecessor_resource_id and
// resource_ids are caught without false-positives like unnormalized_name.
// Diagnostics routes may explicitly allow a small subset below when their ADR
// route contract names lineage/diagnostic identity fields as the point of the
// route. Product-only removals stay in PRODUCT_ONLY_BANNED_FIELD_NAMES.
const BANNED_V1_FIELD_NAMES: &[&str] = &[
    "normalized_name",
    "canonical_display_name",
    "logical_name_id",
    "resource_id",
    "predecessor_resource_id",
    "resource_hex",
    "token_lineage_id",
    "surface_binding_id",
    "binding_kind",
    "normalized_event_id",
    "permission_row",
    "raw_fact_refs",
    "subject",
    "owner_address",
    "registry_owner",
    "token_holder",
    "effective_controller",
    "manager_address",
    "expiry_date",
    "expiration",
    "expiry",
    "registration_date",
    "chain_positions",
    "coin_addresses",
    "coin_type_addresses",
    "resolver_address",
    "current_resolver",
    "mode",
    "consistency",
    "declared_state",
    "verified_state",
    "effective_powers",
    "role_bitmap",
    "authority_epoch",
    "verification_failed",
    "view",
    "contains_nocase",
    "resolved_address",
    "execution_checkpoint",
];

const BANNED_V1_EXACT_FIELD_NAMES: &[&str] = &[
    // ADR 0006 names bare `resource` as a deleted v1 handle spelling. Product
    // resource_* compounds are handled by PRODUCT_ONLY_BANNED_FIELD_NAMES below.
    "resource",
];

// ADR 0006 section "Deleted wire surface" keeps provenance and manifest internals off
// product routes; diagnostics routes carry them by design.
const PRODUCT_ONLY_BANNED_FIELD_NAMES: &[&str] = &[
    "manifest_version",
    "manifest_versions",
    "provenance",
    "raw_log",
    "coverage",
    // Ratified API-boundary rule: product resource_* compounds use
    // registration_* spelling when the prefix names the v1 resource concept.
    "resource",
];

// docs/api-v2-routes.md documents diagnostics events carrying
// normalized_event_id, and ADR 0006 tier-3 diagnostics are the routes that may
// carry pipeline vocabulary. It remains banned on product routes.
const DIAGNOSTICS_ONLY_PIPELINE_IDENTIFIER_FIELD_NAMES: &[&str] = &["normalized_event_id"];

// ADR 0006 section "Rules" bans pipeline vocabulary from product-route field names,
// enum/status values, and errors. Storage table names are enumerated from
// crates/storage plus migrations/20260430060000_baseline.sql and later
// storage migrations.
const PRODUCT_PIPELINE_TERMS: &[&str] = &[
    "projection",
    "sidecar",
    "manifest_version",
    "manifest",
    "normalized_event",
    "normalized event",
    "permission_row",
    "raw_log",
    "raw_fact",
    "raw fact",
    "coverage",
    "resource_authority",
    "resource_rebound",
    "derivation_kind",
    "exhaustiveness",
    "enumeration_basis",
    "source_classes_considered",
    "address_names_current",
    "address_names_current_identity_counts",
    "address_names_current_identity_feed",
    "backfill_jobs",
    "backfill_ranges",
    "chain_checkpoints",
    "chain_header_audit",
    "chain_lineage",
    "children_current",
    "contract_instance_addresses",
    "contract_instances",
    "current_projection_replay_status",
    "discovery_edges",
    "event_silent_resolver_call_observations",
    "execution_cache_outcomes",
    "execution_steps",
    "execution_traces",
    "label_preimage_backfill_runs",
    "label_preimages",
    "manifest_alert_observations",
    "manifest_capability_flags",
    "manifest_contract_instances",
    "manifest_discovery_rules",
    "manifest_versions",
    "name_current",
    "name_surface_normalization_repair_findings",
    "name_surfaces",
    "normalized_events",
    "normalized_replay_adapter_checkpoint_items",
    "normalized_replay_adapter_checkpoints",
    "normalized_replay_cursors",
    "permission_current",
    "permissions_current",
    "primary_names_current",
    "projection_apply_cursors",
    "projection_invalidations",
    "projection_invalidation_dead_letters",
    "projection_normalized_event_changes",
    "raw_call_snapshots",
    "raw_code_hashes",
    "raw_logs",
    "raw_payload_cache_metadata",
    "raw_receipts",
    "raw_transactions",
    "record_inventory_current",
    "resolver_current",
    "resources",
    "surface_bindings",
    "token_lineages",
];

const DIAGNOSTICS_BINDING_DICTIONARY_ALLOWLIST: &[&str] = &[
    // ADR 0006 route catalog says diagnostics binding explains binding ids,
    // binding kind, and anchors.
    "logical_name_id",
    "resource_id",
    "token_lineage_id",
    "surface_binding_id",
    "binding_kind",
];

const DIAGNOSTICS_AUTHORITY_DICTIONARY_ALLOWLIST: &[&str] = &[
    // ADR 0006 route catalog says diagnostics authority explains token
    // lineage, control vectors, and permission lineage.
    "resource_id",
    "token_lineage_id",
    "binding_kind",
    "registry_owner",
];

const DIAGNOSTICS_EVENTS_DICTIONARY_ALLOWLIST: &[&str] = &[
    // docs/api-v2-routes.md L554-L559 documents diagnostics events as raw
    // normalized-event rows with raw fact refs, chain position, and full provenance.
    "normalized_event_id",
    "chain_position",
    "raw_fact_ref",
    "raw_fact_refs",
];

const DIAGNOSTICS_RECORDS_DICTIONARY_ALLOWLIST: &[&str] = &[
    // Diagnostics records expose storage version boundaries; the route is not a
    // product envelope and uses the persisted singular chain_position shape.
    "chain_position",
];

const V2_CONFORMANCE_ROUTES: &[V2ConformanceRoute] = &[
    V2ConformanceRoute {
        label: "POST /v2/lookup",
        error_uri: "/v2/lookup",
        success: V2SuccessFixture::Lookup,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/status",
        error_uri: "/v2/status",
        success: V2SuccessFixture::Status,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Absent,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/names/{name}",
        error_uri: "/v2/names/alice.eth",
        success: V2SuccessFixture::Name,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/names/{name}/records",
        error_uri: "/v2/names/alice.eth/records",
        success: V2SuccessFixture::NameRecords,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/names/{name}/subnames",
        error_uri: "/v2/names/alice.eth/subnames",
        success: V2SuccessFixture::Subnames,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/names/{name}/history",
        error_uri: "/v2/names/alice.eth/history",
        success: V2SuccessFixture::NameHistory,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/permissions",
        error_uri: "/v2/permissions",
        success: V2SuccessFixture::Permissions,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/addresses/{address}/names",
        error_uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/names",
        success: V2SuccessFixture::AddressNames,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/addresses/{address}/primary-name",
        error_uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/primary-name",
        success: V2SuccessFixture::PrimaryName,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Absent,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/addresses/{address}/history",
        error_uri: "/v2/addresses/0x00000000000000000000000000000000000000aa/history",
        success: V2SuccessFixture::AddressHistory,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/search",
        error_uri: "/v2/search",
        success: V2SuccessFixture::Search,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/events",
        error_uri: "/v2/events",
        success: V2SuccessFixture::Events,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/resolvers/{chain_id}/{address}",
        error_uri: "/v2/resolvers/1/0x00000000000000000000000000000000000000aa",
        success: V2SuccessFixture::Resolver,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/namespaces/{namespace}",
        error_uri: "/v2/namespaces/ens",
        success: V2SuccessFixture::Namespace,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Absent,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/names/{name}/coverage",
        error_uri: "/v2/diagnostics/names/alice.eth/coverage",
        success: V2SuccessFixture::DiagnosticsCoverage,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/names/{name}/binding",
        error_uri: "/v2/diagnostics/names/alice.eth/binding",
        success: V2SuccessFixture::DiagnosticsBinding,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: DIAGNOSTICS_BINDING_DICTIONARY_ALLOWLIST,
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/names/{name}/authority",
        error_uri: "/v2/diagnostics/names/alice.eth/authority",
        success: V2SuccessFixture::DiagnosticsAuthority,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: DIAGNOSTICS_AUTHORITY_DICTIONARY_ALLOWLIST,
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/names/{name}/records",
        error_uri: "/v2/diagnostics/names/alice.eth/records",
        success: V2SuccessFixture::DiagnosticsRecords,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: DIAGNOSTICS_RECORDS_DICTIONARY_ALLOWLIST,
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/names/{name}/execution",
        error_uri: "/v2/diagnostics/names/alice.eth/execution",
        success: V2SuccessFixture::DiagnosticsExecution,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/namespaces/{namespace}/manifests",
        error_uri: "/v2/diagnostics/namespaces/ens/manifests",
        success: V2SuccessFixture::DiagnosticsNamespaceManifests,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Absent,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: &[],
    },
    V2ConformanceRoute {
        label: "GET /v2/diagnostics/events",
        error_uri: "/v2/diagnostics/events",
        success: V2SuccessFixture::DiagnosticsEvents,
        envelope: V2TopLevelEnvelope::DataPageMeta,
        as_of: V2AsOfExpectation::Present,
        tier: V2RouteTier::Diagnostics,
        dictionary_allowlist: DIAGNOSTICS_EVENTS_DICTIONARY_ALLOWLIST,
    },
];

#[tokio::test]
async fn v2_success_envelopes_conform_family_wide() -> Result<()> {
    assert_v2_conformance_route_tables_match();

    for route in V2_CONFORMANCE_ROUTES {
        let payload = v2_conformance_success_payload(route).await?;
        assert_v2_success_envelope(route, &payload);
    }

    Ok(())
}

#[tokio::test]
async fn v2_error_envelopes_conform_family_wide() -> Result<()> {
    assert_v2_conformance_route_tables_match();

    let database = TestDatabase::new_migrated().await?;

    for route in V2_CONFORMANCE_ROUTES {
        let case = v2_conformance_strict_query_case(route);
        let response = v2_strict_query_response(&database, case).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{}", route.label);

        let payload: Value = read_json(response).await?;
        assert_object_keys(&payload, &["error"], route.label);
        assert_object_keys(
            &payload["error"],
            &["code", "message", "details"],
            route.label,
        );
        assert_eq!(payload["error"]["code"], json!("invalid_input"), "{}", route.label);
        assert_eq!(
            payload["error"]["message"],
            json!(case.expected_message),
            "{}",
            route.label
        );
        assert!(
            payload["error"]["details"].is_object(),
            "{} error details must be an object",
            route.label
        );
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_success_responses_omit_banned_v1_dictionary_fields_family_wide() -> Result<()> {
    assert_v2_conformance_route_tables_match();

    let mut violations = Vec::new();
    for route in V2_CONFORMANCE_ROUTES {
        let payload = v2_conformance_success_payload(route).await?;
        collect_banned_dictionary_fields(route, &payload, &mut violations);
    }

    assert_no_conformance_violations("v2 dictionary conformance", &violations);
    Ok(())
}

#[tokio::test]
async fn v2_product_routes_hide_pipeline_vocabulary_family_wide() -> Result<()> {
    assert_v2_conformance_route_tables_match();

    let mut violations = Vec::new();

    for route in V2_CONFORMANCE_ROUTES
        .iter()
        .filter(|route| route.tier == V2RouteTier::Product)
    {
        let payload = v2_conformance_success_payload(route).await?;
        collect_pipeline_vocabulary_in_product_response(route, &payload, &mut violations);
    }

    let database = TestDatabase::new_migrated().await?;
    for route in V2_CONFORMANCE_ROUTES
        .iter()
        .filter(|route| route.tier == V2RouteTier::Product)
    {
        let response = v2_strict_query_response(&database, v2_conformance_strict_query_case(route))
            .await?;
        let payload: Value = read_json(response).await?;
        collect_pipeline_vocabulary_in_error_message(route, &payload, &mut violations);
    }

    database.cleanup().await?;
    assert_no_conformance_violations("v2 product pipeline-vocabulary conformance", &violations);
    Ok(())
}

async fn v2_conformance_success_payload(route: &V2ConformanceRoute) -> Result<Value> {
    match route.success {
        V2SuccessFixture::Lookup => {
            let database = TestDatabase::new_migrated().await?;
            seed_identity_name(
                &database,
                "ens:case.eth",
                "Case.eth",
                "case.eth",
                "namehash:case.eth",
                Uuid::from_u128(0x5a0101),
                Uuid::from_u128(0x5a0102),
                Uuid::from_u128(0x5a0103),
                "0x0000000000000000000000000000000000000abc",
                bigname_storage::AddressNameRelation::TokenHolder,
                38,
            )
            .await?;
            let payload = v2_lookup_json(
                &database,
                json!({
                    "profile": "detail",
                    "namespace": "public",
                    "inputs": [{"id": "hit", "name": "Case.eth"}]
                }),
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Status => {
            let database = TestDatabase::new_migrated().await?;
            let payload = v2_conformance_get_json(&database, "/v2/status").await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Name => v2_name_record_payload("/v2/names/Alice.eth").await,
        V2SuccessFixture::NameRecords => {
            v2_name_records_payload_with_setup(
                "/v2/names/Alice.eth/records?keys=addr:60&include=inventory",
                |_, _, inventory| {
                    let entries = inventory
                        .entries
                        .as_array_mut()
                        .expect("record inventory entries must be an array");
                    entries[0] = json!({
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "unsupported",
                        "unsupported_reason": "value_not_retained_in_normalized_events"
                    });
                },
                None,
            )
            .await
        }
        V2SuccessFixture::Subnames => {
            let (database, payload) =
                v2_subnames_payload("/v2/names/Parent.eth/subnames?include=counts&page_size=3")
                    .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::NameHistory => {
            let (database, payload) =
                v2_history_payload("/v2/names/History.eth/history?page_size=20").await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Permissions => {
            let (database, payload) = v2_permissions_payload(&format!(
                "/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=10"
            ))
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::AddressNames => {
            let (database, payload) = v2_address_names_payload(&format!(
                "/v2/addresses/{V2_ADDRESS}/names?include=role_summary"
            ))
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::PrimaryName => {
            let database = TestDatabase::new_migrated().await?;
            database
                .insert_primary_name_current_claim_row(
                    V2_PRIMARY_NAME_ADDRESS,
                    "ens",
                    "60",
                    PrimaryNameClaimStatus::Success,
                    None,
                )
                .await?;
            database
                .insert_primary_name_current_normalized_claim_name(
                    V2_PRIMARY_NAME_ADDRESS,
                    "ens",
                    "60",
                    Some("alice.eth"),
                )
                .await?;
            let payload = v2_primary_name_payload_for_database(
                &database,
                &format!("/v2/addresses/{V2_PRIMARY_NAME_ADDRESS}/primary-name"),
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::AddressHistory => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_address_history_conformance_fixture(&database).await?;
            let payload = v2_conformance_get_json(
                &database,
                &format!("/v2/addresses/{V2_ADDRESS}/history?page_size=20"),
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Search => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_address_names_fixture(&database).await?;
            let payload =
                v2_conformance_get_json(&database, "/v2/search?q=alpha&namespace=ens").await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Events => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_history_fixture(&database).await?;
            let payload =
                v2_conformance_get_json(&database, "/v2/events?name=history.eth&page_size=20")
                    .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Resolver => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_resolver_bound_names_fixture(&database).await?;
            let mut resolver_row =
                resolver_current_row_with_writer_alias("ethereum-mainnet", V2_RESOLVER_ADDRESS);
            resolver_row.declared_summary["role_holders"]["items"][0]["effective_powers"] =
                json!(["resource_control", "set_resolver"]);
            bigname_storage::upsert_resolver_current_rows(
                &database.pool,
                &[resolver_row],
            )
            .await?;
            let payload = v2_resolver_payload_for_database(
                &database,
                &format!(
                    "/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes,aliases,roles,events&page_size=5"
                ),
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Namespace => {
            let database = TestDatabase::new(true).await?;
            seed_v2_conformance_namespace_manifests(&database).await?;
            let payload = v2_conformance_get_json(&database, "/v2/namespaces/ens").await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsCoverage
        | V2SuccessFixture::DiagnosticsBinding
        | V2SuccessFixture::DiagnosticsAuthority => {
            let suffix = match route.success {
                V2SuccessFixture::DiagnosticsCoverage => "coverage",
                V2SuccessFixture::DiagnosticsBinding => "binding",
                V2SuccessFixture::DiagnosticsAuthority => "authority",
                _ => unreachable!("matched above"),
            };
            let database = TestDatabase::new_with_schemas(false, true).await?;
            seed_v2_diagnostics_name_fixture(&database, "ens:alice.eth", 21_000_003).await?;
            let payload = request_v2_diagnostics_json(
                &database,
                &format!("/v2/diagnostics/names/Alice.eth/{suffix}"),
                StatusCode::OK,
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsRecords => {
            let database = TestDatabase::new_with_schemas(false, true).await?;
            seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;
            let payload = request_v2_diagnostics_json(
                &database,
                "/v2/diagnostics/names/Alice.eth/records",
                StatusCode::OK,
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsExecution => {
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
                verified_queries,
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
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsNamespaceManifests => {
            let database = TestDatabase::new(true).await?;
            seed_v2_conformance_namespace_manifests(&database).await?;
            let payload = v2_conformance_get_json(
                &database,
                "/v2/diagnostics/namespaces/ens/manifests",
            )
            .await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsEvents => {
            let (database, payload) =
                v2_diag_events_payload("/v2/diagnostics/events?name=Diag.eth&page_size=10")
                    .await?;
            database.cleanup().await?;
            Ok(payload)
        }
    }
}

async fn v2_conformance_get_json(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .with_context(|| format!("v2 conformance request failed for {uri}"))?;
    let status = response.status();
    let payload = read_json(response).await?;

    assert_eq!(status, StatusCode::OK, "{uri}: {payload}");
    Ok(payload)
}

async fn seed_v2_address_history_conformance_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_address_names_fixture(database).await?;

    let alpha = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.logical_name_id == "ens:alpha.eth")
        .expect("alpha address-name fixture must exist");
    let mut surface_event = history_event(
        "v2-address-history-surface",
        Some(alpha.logical_name_id),
        None,
        Some("ethereum-mainnet"),
        Some(alpha.block_number),
        Some(alpha.block_hash),
        Some("0xv2addrhist01"),
        Some(0),
        CanonicalityState::Canonical,
    );
    surface_event.event_kind = "ResolverChanged".to_owned();
    let mut resource_event = history_event(
        "v2-address-history-resource",
        None,
        Some(alpha.resource_id),
        Some("ethereum-mainnet"),
        Some(alpha.block_number),
        Some(alpha.block_hash),
        Some("0xv2addrhist02"),
        Some(1),
        CanonicalityState::Canonical,
    );
    resource_event.event_kind = "RegistrationRenewed".to_owned();

    bigname_storage::upsert_normalized_events(&database.pool, &[surface_event, resource_event])
        .await?;
    Ok(())
}

async fn seed_v2_conformance_namespace_manifests(database: &TestDatabase) -> Result<()> {
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
        .insert_capability_flag(ens_l1, "verified_resolution", "supported", None)
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
    Ok(())
}

fn assert_v2_success_envelope(route: &V2ConformanceRoute, payload: &Value) {
    match route.envelope {
        V2TopLevelEnvelope::DataMeta => assert_object_keys(payload, &["data", "meta"], route.label),
        V2TopLevelEnvelope::DataPageMeta => {
            assert_object_keys(payload, &["data", "meta", "page"], route.label);
            assert_object_keys(
                &payload["page"],
                &["cursor", "next_cursor", "page_size", "total_count", "has_more"],
                route.label,
            );
        }
    }

    assert!(
        payload["meta"].is_object(),
        "{} meta must be an object",
        route.label
    );
    match route.as_of {
        V2AsOfExpectation::Present => assert_as_of_shape(route, &payload["meta"]["as_of"]),
        V2AsOfExpectation::Absent => assert!(
            payload["meta"].get("as_of").is_none(),
            "{} must omit meta.as_of",
            route.label
        ),
    }

    assert_non_empty_json(&payload["data"], route.label, "$.data");
    assert_v2_exercised_expansions_non_empty(route, payload);
}

fn assert_v2_exercised_expansions_non_empty(route: &V2ConformanceRoute, payload: &Value) {
    match route.success {
        V2SuccessFixture::NameRecords => {
            assert_non_empty_json(&payload["data"]["records"], route.label, "$.data.records");
            assert_non_empty_json(
                &payload["data"]["inventory"],
                route.label,
                "$.data.inventory",
            );
            assert_non_empty_json(
                &payload["data"]["inventory"]["known_keys"],
                route.label,
                "$.data.inventory.known_keys",
            );
            assert_eq!(
                payload["data"]["records"]["addr:60"]["unsupported_reason"],
                json!("value_not_retained"),
                "{} records fixture must exercise product reason mapping",
                route.label
            );
        }
        V2SuccessFixture::Subnames => {
            assert!(
                payload["data"][0]["subname_count"].is_u64(),
                "{} include=counts must populate data[0].subname_count",
                route.label
            );
        }
        V2SuccessFixture::Permissions => {
            let rows = payload["data"]
                .as_array()
                .unwrap_or_else(|| panic!("{} data must be an array", route.label));
            assert!(
                rows.iter()
                    .any(|row| row.get("lineage").is_some_and(json_value_is_non_empty)),
                "{} include=lineage must populate at least one non-empty lineage section",
                route.label
            );
        }
        V2SuccessFixture::AddressNames => {
            let rows = payload["data"]
                .as_array()
                .unwrap_or_else(|| panic!("{} data must be an array", route.label));
            assert!(
                rows.iter()
                    .any(|row| row.get("role_summary").is_some_and(json_value_is_non_empty)),
                "{} include=role_summary must populate at least one non-empty role_summary section",
                route.label
            );
        }
        V2SuccessFixture::Resolver => {
            for key in ["nodes", "aliases", "roles", "events"] {
                assert_non_empty_json(&payload["data"][key], route.label, &format!("$.data.{key}"));
            }
        }
        V2SuccessFixture::DiagnosticsRecords => {
            for key in ["record_inventory", "record_cache", "value_sources", "comparison"] {
                assert_non_empty_json(&payload["data"][key], route.label, &format!("$.data.{key}"));
            }
        }
        _ => {}
    }
}

fn assert_non_empty_json(value: &Value, context: &str, path: &str) {
    assert!(
        json_value_is_non_empty(value),
        "{context} {path} must be non-empty"
    );
}

fn json_value_is_non_empty(value: &Value) -> bool {
    match value {
        Value::Object(object) => !object.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::String(text) => !text.is_empty(),
        Value::Null => false,
        Value::Bool(_) | Value::Number(_) => true,
    }
}

fn assert_as_of_shape(route: &V2ConformanceRoute, as_of: &Value) {
    let chains = as_of
        .as_object()
        .unwrap_or_else(|| panic!("{} meta.as_of must be an object", route.label));
    assert!(
        !chains.is_empty(),
        "{} meta.as_of must include at least one chain",
        route.label
    );

    for (chain_id, position) in chains {
        assert!(
            !chain_id.is_empty() && chain_id.chars().all(|ch| ch.is_ascii_digit()),
            "{} meta.as_of key {chain_id:?} must be a string chain id",
            route.label
        );
        assert_object_keys(
            position,
            &["block_number", "block_hash", "timestamp"],
            route.label,
        );
        assert!(
            position["block_number"].is_i64() || position["block_number"].is_u64(),
            "{} meta.as_of[{chain_id}].block_number must be numeric",
            route.label
        );
        assert!(
            position["block_hash"].is_string(),
            "{} meta.as_of[{chain_id}].block_hash must be a string",
            route.label
        );
        assert!(
            position["timestamp"].is_string(),
            "{} meta.as_of[{chain_id}].timestamp must be a string",
            route.label
        );
    }
}

fn collect_banned_dictionary_fields(
    route: &V2ConformanceRoute,
    value: &Value,
    violations: &mut Vec<String>,
) {
    walk_json_fields(value, "$", &mut |path, key| {
        if is_dictionary_allowlisted(route, path, key) {
            return;
        }

        for term in matched_banned_dictionary_field_names(key) {
            violations.push(format!(
                "{} at {path}: field {key:?} matches banned v1 field term {term:?}",
                route.label
            ));
        }

        if route.tier == V2RouteTier::Product {
            for term in matched_field_name_terms(key, PRODUCT_ONLY_BANNED_FIELD_NAMES) {
                violations.push(format!(
                    "{} at {path}: field {key:?} matches product-banned field term {term:?}",
                    route.label
                ));
            }
        }
    });
}

fn collect_pipeline_vocabulary_in_product_response(
    route: &V2ConformanceRoute,
    value: &Value,
    violations: &mut Vec<String>,
) {
    walk_product_pipeline_response(value, "$", None, &mut |path, value_key, candidate| {
        for term in matched_pipeline_terms(candidate) {
            violations.push(format!(
                "{} at {path}: {candidate:?} contains product-banned pipeline vocabulary {term:?}",
                route.label
            ));
        }

        if value_key == Some("chain_id") && !candidate.bytes().all(|byte| byte.is_ascii_digit()) {
            violations.push(format!(
                "{} at {path}: chain_id value {candidate:?} must use a numeric string on product routes",
                route.label
            ));
        }

        if value_key == Some("powers") && candidate == "resource_control" {
            violations.push(format!(
                "{} at {path}: powers value {candidate:?} uses storage permission vocabulary",
                route.label
            ));
        }
    });
}

fn collect_pipeline_vocabulary_in_error_message(
    route: &V2ConformanceRoute,
    payload: &Value,
    violations: &mut Vec<String>,
) {
    let message = payload["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("{} error message must be a string", route.label));
    for term in matched_pipeline_terms(message) {
        violations.push(format!(
            "{} error message: {message:?} contains product-banned pipeline vocabulary {term:?}",
            route.label
        ));
    }
}

fn assert_no_conformance_violations(context: &str, violations: &[String]) {
    if violations.is_empty() {
        return;
    }

    let mut message = format!("{context} found {} violation(s):", violations.len());
    for violation in violations {
        message.push_str("\n- ");
        message.push_str(violation);
    }
    panic!("{message}");
}

fn walk_json_fields(
    value: &Value,
    path: &str,
    visit: &mut impl FnMut(&str, &str),
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = json_path(path, key);
                visit(&child_path, key);
                walk_json_fields(child, &child_path, visit);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                walk_json_fields(child, &format!("{path}[{index}]"), visit);
            }
        }
        _ => {}
    }
}

fn walk_product_pipeline_response(
    value: &Value,
    path: &str,
    value_key: Option<&str>,
    visit: &mut impl FnMut(&str, Option<&str>, &str),
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = json_path(path, key);
                visit(&child_path, None, key);
                walk_product_pipeline_response(child, &child_path, Some(key), visit);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                walk_product_pipeline_response(child, &format!("{path}[{index}]"), value_key, visit);
            }
        }
        Value::String(text)
            if value_key.is_some_and(|key| {
                is_enumish_product_value_key(key) || key == "chain_id" || key == "powers"
            }) =>
        {
            visit(path, value_key, text);
        }
        _ => {}
    }
}

fn is_enumish_product_value_key(key: &str) -> bool {
    key == "type"
        || key == "kind"
        || key == "status"
        || key == "source"
        || key == "scope"
        || key == "completeness"
        || key == "relation"
        || key == "relations"
        || key == "powers"
        || key.ends_with("_type")
        || key.ends_with("_kind")
        || key.ends_with("_status")
        || key.ends_with("_source")
        || key.ends_with("_scope")
        || key.ends_with("_reason")
}

fn matched_pipeline_terms(candidate: &str) -> Vec<&'static str> {
    let normalized_candidate = normalize_pipeline_candidate(candidate);

    PRODUCT_PIPELINE_TERMS
        .iter()
        .copied()
        .filter(|term| {
            let normalized_term = normalize_pipeline_candidate(term);
            normalized_candidate.contains(&normalized_term)
        })
        .collect()
}

fn normalize_pipeline_candidate(candidate: &str) -> String {
    candidate
        .chars()
        .map(|ch| match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            '-' | ' ' => '_',
            _ => ch,
        })
        .collect()
}

fn is_dictionary_allowlisted(route: &V2ConformanceRoute, path: &str, key: &str) -> bool {
    if route.success == V2SuccessFixture::DiagnosticsEvents
        && diagnostics_events_raw_state_subtree(path)
    {
        return true;
    }

    route.dictionary_allowlist.contains(&key)
        || (route.tier == V2RouteTier::Diagnostics
            && !matched_field_name_terms(key, DIAGNOSTICS_ONLY_PIPELINE_IDENTIFIER_FIELD_NAMES)
                .is_empty())
}

fn diagnostics_events_raw_state_subtree(path: &str) -> bool {
    path.contains(".before_state.") || path.contains(".after_state.")
}

fn matched_banned_dictionary_field_names(key: &str) -> Vec<&'static str> {
    let mut matches = BANNED_V1_EXACT_FIELD_NAMES
        .iter()
        .copied()
        .filter(|term| *term == key)
        .collect::<Vec<_>>();
    matches.extend(matched_field_name_terms(key, BANNED_V1_FIELD_NAMES));
    matches
}

fn matched_field_name_terms(key: &str, terms: &'static [&'static str]) -> Vec<&'static str> {
    terms
        .iter()
        .copied()
        .filter(|term| field_name_term_matches(key, term))
        .collect()
}

fn field_name_term_matches(key: &str, term: &str) -> bool {
    field_name_term_variants(term)
        .iter()
        .any(|variant| key_has_underscore_boundary_term(key, variant))
}

fn field_name_term_variants(term: &str) -> Vec<String> {
    let mut variants = vec![term.to_owned(), format!("{term}s"), format!("{term}es")];
    if let Some(singular) = term.strip_suffix('s') {
        variants.push(singular.to_owned());
    }
    variants.sort_unstable();
    variants.dedup();
    variants
}

fn key_has_underscore_boundary_term(key: &str, term: &str) -> bool {
    key.match_indices(term)
        .any(|(start, _)| term_match_has_underscore_boundaries(key, term, start))
}

fn term_match_has_underscore_boundaries(key: &str, term: &str, start: usize) -> bool {
    let before_is_boundary = start == 0 || key.as_bytes()[start - 1] == b'_';
    if !before_is_boundary {
        return false;
    }

    let end = start + term.len();
    if end == key.len() || key.as_bytes()[end] == b'_' {
        return true;
    }

    key.as_bytes()[end] == b's'
        && (end + 1 == key.len() || key.as_bytes()[end + 1] == b'_')
}

#[test]
fn v2_dictionary_field_matching_uses_underscore_boundaries_and_plural_suffixes() {
    assert_eq!(
        matched_banned_dictionary_field_names("resource_ids"),
        vec!["resource_id"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("predecessor_resource_ids"),
        vec!["resource_id", "predecessor_resource_id"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("last_normalized_event_id"),
        vec!["normalized_event_id"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("permission_row_count"),
        vec!["permission_row"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("owner_addresses"),
        vec!["owner_address"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("chain_position"),
        vec!["chain_positions"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("raw_fact_ref"),
        vec!["raw_fact_refs"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("subject"),
        vec!["subject"]
    );
    assert_eq!(
        matched_banned_dictionary_field_names("resource"),
        vec!["resource"]
    );
    assert!(matched_banned_dictionary_field_names("registration_ids").is_empty());
    assert!(matched_banned_dictionary_field_names("registration_count").is_empty());
    assert_eq!(
        matched_field_name_terms("resource_count", PRODUCT_ONLY_BANNED_FIELD_NAMES),
        vec!["resource"]
    );
    assert!(matched_banned_dictionary_field_names("unnormalized_name").is_empty());
}

#[test]
fn v2_product_value_matching_targets_chain_ids_and_storage_powers() {
    let route = V2ConformanceRoute {
        label: "test",
        error_uri: "/test",
        success: V2SuccessFixture::Status,
        envelope: V2TopLevelEnvelope::DataMeta,
        as_of: V2AsOfExpectation::Absent,
        tier: V2RouteTier::Product,
        dictionary_allowlist: &[],
    };
    let payload = json!({
        "data": {
            "chain_id": "ethereum-mainnet",
            "powers": ["resource_control", "resolver_control"]
        },
        "meta": {}
    });
    let mut violations = Vec::new();

    collect_pipeline_vocabulary_in_product_response(&route, &payload, &mut violations);

    assert_eq!(violations.len(), 2);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("chain_id value"))
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("resource_control"))
    );
}

fn assert_object_keys(value: &Value, expected: &[&str], context: &str) {
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("{context} must be a JSON object"));
    let mut actual = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{context} object keys");
}

fn json_path(parent: &str, key: &str) -> String {
    if parent == "$" {
        format!("$.{key}")
    } else {
        format!("{parent}.{key}")
    }
}

fn v2_conformance_strict_query_case(route: &V2ConformanceRoute) -> &'static V2StrictQueryCase {
    V2_STRICT_QUERY_CASES
        .iter()
        .find(|case| case.uri == route.error_uri)
        .unwrap_or_else(|| panic!("{} is missing from strict-query conformance table", route.label))
}

fn assert_v2_conformance_route_tables_match() {
    let conformance_uris = V2_CONFORMANCE_ROUTES
        .iter()
        .map(|route| route.error_uri)
        .collect::<Vec<_>>();
    let strict_query_uris = V2_STRICT_QUERY_CASES
        .iter()
        .map(|case| case.uri)
        .collect::<Vec<_>>();

    assert_eq!(
        conformance_uris, strict_query_uris,
        "v2 conformance route table must cover the same registered routes as v2_query_params"
    );
}
