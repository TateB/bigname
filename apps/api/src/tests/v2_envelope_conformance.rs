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
    Conditional,
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
        as_of: V2AsOfExpectation::Conditional,
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
        assert_v2_error_envelope(route.label, &payload, "invalid_input");
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
async fn v2_address_history_filters_relation_sets_and_defaults_namespace() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_history_conformance_fixture(&database).await?;
    let beta = v2_address_name_specs()
        .into_iter()
        .find(|spec| spec.logical_name_id == "ens:beta.eth")
        .expect("beta address-name fixture must exist");
    let mut beta_event = history_event(
        "v2-address-history-beta-resource",
        None,
        Some(beta.resource_id),
        Some("ethereum-mainnet"),
        Some(beta.block_number),
        Some(beta.block_hash),
        Some("0xv2addrhist03"),
        Some(2),
        CanonicalityState::Canonical,
    );
    beta_event.event_kind = "RegistrationRenewed".to_owned();
    bigname_storage::upsert_normalized_events(&database.pool, &[beta_event]).await?;

    let set_payload = v2_conformance_get_json(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/history?relation=registrant,manager&page_size=20"),
    )
    .await?;
    let set_rows = set_payload["data"]
        .as_array()
        .expect("address-history data must be an array");
    assert!(
        set_rows.iter().all(|row| row["namespace"] == json!("ens")),
        "omitted namespace must default address history to ens"
    );
    assert!(
        set_rows
            .iter()
            .any(|row| row["transaction_hash"] == json!("0xv2addrhist02")),
        "registrant half of the relation set must match alpha resource history"
    );
    assert!(
        set_rows
            .iter()
            .any(|row| row["transaction_hash"] == json!("0xv2addrhist03")),
        "manager half of the relation set must match beta resource history"
    );

    let owner_payload = v2_conformance_get_json(
        &database,
        &format!("/v2/addresses/{V2_ADDRESS}/history?relation=owner&page_size=20"),
    )
    .await?;
    let owner_rows = owner_payload["data"]
        .as_array()
        .expect("owner address-history data must be an array");
    assert!(
        owner_rows
            .iter()
            .all(|row| row["transaction_hash"] != json!("0xv2addrhist03")),
        "owner-only relation filter must exclude manager-only beta history"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_product_error_bodies_hide_pipeline_vocabulary_for_not_found_stale_conflict_and_internal(
) -> Result<()> {
    assert_v2_conformance_route_tables_match();

    let mut violations = Vec::new();

    collect_v2_stale_and_conflict_error_violations(&mut violations).await?;
    collect_v2_not_found_error_violations(&mut violations).await?;
    collect_v2_internal_error_violations(&mut violations).await?;

    assert_no_conformance_violations("v2 product error-body conformance", &violations);
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
async fn v2_single_chain_as_of_token_replays_across_route_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row_with_writer_alias(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;

    let minted = v2_conformance_get_json(&database, "/v2/names/alpha.eth").await?;
    let token = minted["meta"]["as_of_token"]
        .as_str()
        .expect("name response must include meta.as_of_token");

    for (label, uri) in [
        (
            "records",
            format!("/v2/names/alpha.eth/records?at={token}"),
        ),
        (
            "subnames",
            format!("/v2/names/alpha.eth/subnames?at={token}"),
        ),
        (
            "history",
            format!("/v2/names/alpha.eth/history?at={token}"),
        ),
        (
            "namespace-scoped search",
            format!("/v2/search?q=alpha&namespace=ens&at={token}"),
        ),
        (
            "resolver",
            format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?at={token}"),
        ),
    ] {
        let replay = v2_conformance_get_json(&database, &uri).await?;
        assert_eq!(
            replay["meta"]["as_of"], minted["meta"]["as_of"],
            "{label} must accept name-route as_of_token"
        );
        assert_eq!(
            replay["meta"]["as_of_token"], minted["meta"]["as_of_token"],
            "{label} must preserve name-route as_of_token"
        );
    }

    database.cleanup().await?;
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

    let records_route = V2_CONFORMANCE_ROUTES
        .iter()
        .find(|route| route.success == V2SuccessFixture::NameRecords)
        .expect("name-records conformance route must be registered");
    let verified_stale_records = v2_conformance_name_records_verified_stale_payload().await?;
    assert_v2_name_records_verified_stale_fixture(&verified_stale_records);
    collect_pipeline_vocabulary_in_product_response(
        records_route,
        &verified_stale_records,
        &mut violations,
    );

    let database = TestDatabase::new_migrated().await?;
    for route in V2_CONFORMANCE_ROUTES
        .iter()
        .filter(|route| route.tier == V2RouteTier::Product)
    {
        let response = v2_strict_query_response(&database, v2_conformance_strict_query_case(route))
            .await?;
        let payload: Value = read_json(response).await?;
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, &mut violations);
    }

    database.cleanup().await?;
    assert_no_conformance_violations("v2 product pipeline-vocabulary conformance", &violations);
    Ok(())
}

#[tokio::test]
async fn v2_flat_record_shape_matches_profile_lookup_and_family_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let token_holder_address = "0x0000000000000000000000000000000000000def";
    let logical_name_id = "ens:case.eth";
    let resource_id = Uuid::from_u128(0x5a0101);
    let token_lineage_id = Uuid::from_u128(0x5a0102);
    let surface_binding_id = Uuid::from_u128(0x5a0103);
    let record_boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": null,
        "event_kind": null,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": 38,
            "block_hash": "0xname26",
            "timestamp": "2026-04-17T00:00:38Z"
        }
    });

    seed_identity_name(
        &database,
        logical_name_id,
        "Case.eth",
        "case.eth",
        "namehash:case.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
        address,
        bigname_storage::AddressNameRelation::EffectiveController,
        38,
    )
    .await?;
    bigname_storage::upsert_address_names_current_rows(
        &database.pool,
        &[address_name_current_row(
            token_holder_address,
            logical_name_id,
            bigname_storage::AddressNameRelation::TokenHolder,
            "Case.eth",
            "case.eth",
            "namehash:case.eth",
            surface_binding_id,
            resource_id,
            Some(token_lineage_id),
            38,
        )],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE name_current
        SET declared_summary = jsonb_set(
            declared_summary,
            '{topology}',
            $2::jsonb
        )
        WHERE logical_name_id = $1
        "#,
    )
    .bind(logical_name_id)
    .bind(json!({
        "version_boundaries": {
            "topology_version_boundary": record_boundary.clone(),
            "record_version_boundary": record_boundary.clone(),
        }
    }))
    .execute(&database.pool)
    .await?;
    let mut record_inventory = compact_records_inventory_current_row(logical_name_id, resource_id);
    record_inventory.record_version_boundary = record_boundary;
    record_inventory.selectors = json!([]);
    record_inventory.explicit_gaps = json!([]);
    record_inventory.entries = json!([]);
    record_inventory.unsupported_families = json!([]);
    record_inventory.chain_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 38,
            "block_hash": "0xname26",
            "timestamp": "2026-04-17T00:00:38Z"
        }
    });
    database
        .insert_record_inventory_current_row(record_inventory)
        .await?;

    let profile = v2_conformance_get_json(&database, "/v2/names/Case.eth").await?;
    let lookup = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "public",
            "inputs": [{"name": "Case.eth"}]
        }),
    )
    .await?;
    assert_eq!(lookup["data"][0]["record"], profile["data"]);
    assert_eq!(profile["data"]["owner"], json!(address));
    assert_ne!(profile["data"]["owner"], json!(token_holder_address));
    assert!(
        lookup["data"][0]["record"].get("manager").is_none(),
        "forward relation context must not synthesize the flat manager field"
    );
    assert_eq!(profile["data"]["addresses"], json!({}));
    assert_eq!(profile["data"]["text_records"], json!({}));
    assert_eq!(lookup["data"][0]["record"]["addresses"], json!({}));
    assert_eq!(lookup["data"][0]["record"]["text_records"], json!({}));

    let unbacked_resource_id = Uuid::from_u128(0x5a0111);
    seed_identity_name(
        &database,
        "ens:unbacked.eth",
        "Unbacked.eth",
        "unbacked.eth",
        "namehash:unbacked.eth",
        unbacked_resource_id,
        Uuid::from_u128(0x5a0112),
        Uuid::from_u128(0x5a0113),
        address,
        bigname_storage::AddressNameRelation::TokenHolder,
        39,
    )
    .await?;
    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(unbacked_resource_id)
    .execute(&database.pool)
    .await?;
    let unbacked_profile = v2_conformance_get_json(&database, "/v2/names/Unbacked.eth").await?;
    let unbacked_lookup = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "namespace": "public",
            "inputs": [{"name": "Unbacked.eth"}]
        }),
    )
    .await?;
    assert!(unbacked_profile["data"].get("addresses").is_none());
    assert!(unbacked_profile["data"].get("text_records").is_none());
    assert!(
        unbacked_lookup["data"][0]["record"]
            .get("addresses")
            .is_none()
    );
    assert!(
        unbacked_lookup["data"][0]["record"]
            .get("text_records")
            .is_none()
    );

    let profile_record = &profile["data"];
    let search = v2_conformance_get_json(&database, "/v2/search?q=case&namespace=ens").await?;
    assert_shared_record_subset(
        profile_record,
        data_row_named(&search, "case.eth", "search"),
        SHARED_LIST_RECORD_FIELDS,
        "search",
    );

    let address_names = v2_address_names_payload_for_database(
        &database,
        &format!("/v2/addresses/{address}/names"),
    )
    .await?;
    let address_name_row = data_row_named(&address_names, "case.eth", "address-names");
    assert_eq!(address_name_row["relations"], json!(["manager"]));
    assert_shared_record_subset(
        profile_record,
        address_name_row,
        SHARED_LIST_RECORD_FIELDS,
        "address-names",
    );

    seed_v2_subnames_fixture(&database).await?;
    let subname_at = v2_at_token(
        "ethereum",
        "ethereum-mainnet",
        81,
        "0xname51",
        "2026-04-17T00:00:21Z",
    )?;
    let subname_profile = v2_conformance_get_json(
        &database,
        &format!("/v2/names/Alpha.Parent.eth?at={subname_at}"),
    )
    .await?;
    let subnames =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?page_size=3")
            .await?;
    assert_shared_record_subset(
        &subname_profile["data"],
        data_row_named(&subnames, "alpha.parent.eth", "subnames"),
        SHARED_LIST_RECORD_FIELDS,
        "subnames",
    );

    database.cleanup().await?;
    Ok(())
}

async fn assert_v2_as_of_token_fixpoint(
    database: &TestDatabase,
    route: &V2ConformanceRoute,
    uri: &str,
    payload: &Value,
) -> Result<()> {
    if !route_accepts_at(route) {
        return Ok(());
    }

    let token = payload["meta"]["as_of_token"]
        .as_str()
        .unwrap_or_else(|| panic!("{} must include meta.as_of_token", route.label));
    let replay_uri = uri_with_at(uri, token);
    let replay = v2_conformance_get_json(database, &replay_uri).await?;

    assert_eq!(
        replay["meta"]["as_of"], payload["meta"]["as_of"],
        "{} at-token replay must preserve meta.as_of",
        route.label
    );
    assert_eq!(
        replay["meta"]["as_of_token"], payload["meta"]["as_of_token"],
        "{} at-token replay must preserve meta.as_of_token",
        route.label
    );

    Ok(())
}

fn route_accepts_at(route: &V2ConformanceRoute) -> bool {
    matches!(
        route.as_of,
        V2AsOfExpectation::Present | V2AsOfExpectation::Conditional
    )
        && !matches!(
            route.success,
            V2SuccessFixture::Lookup | V2SuccessFixture::PrimaryName
        )
}

fn uri_with_at(uri: &str, token: &str) -> String {
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}at={token}")
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
            seed_identity_name(
                &database,
                "ens:unsupported.eth",
                "Unsupported.eth",
                "unsupported.eth",
                "namehash:unsupported.eth",
                Uuid::from_u128(0x5a0121),
                Uuid::from_u128(0x5a0122),
                Uuid::from_u128(0x5a0123),
                "0x0000000000000000000000000000000000000abc",
                bigname_storage::AddressNameRelation::TokenHolder,
                39,
            )
            .await?;
            sqlx::query(
                r#"
                UPDATE name_current
                SET coverage = $2::jsonb
                WHERE logical_name_id = $1
                "#,
            )
            .bind("ens:unsupported.eth")
            .bind(json!({
                "status": "unsupported",
                "unsupported_reason": "ensv2_exact_name_profile_shadow"
            }))
            .execute(&database.pool)
            .await?;
            let payload = v2_lookup_json(
                &database,
                json!({
                    "profile": "detail",
                    "namespace": "public",
                    "inputs": [
                        {"id": "hit", "name": "Case.eth"},
                        {"id": "unsupported", "name": "Unsupported.eth"}
                    ]
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
        V2SuccessFixture::Name => {
            let database = TestDatabase::new_with_schemas(false, true).await?;
            let uri = "/v2/names/Alice.eth";
            seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;
            let payload = v2_conformance_get_json(&database, uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::NameRecords => {
            let database = TestDatabase::new_with_schemas(false, true).await?;
            let uri = "/v2/names/Alice.eth/records?keys=addr:60&include=inventory";
            seed_v2_alice_name_records_fixture(
                &database,
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
            .await?;
            let payload = v2_conformance_get_json(&database, uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Subnames => {
            let uri = "/v2/names/Parent.eth/subnames?include=counts&page_size=3";
            let (database, payload) =
                v2_subnames_payload(uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::NameHistory => {
            let uri = "/v2/names/History.eth/history?page_size=20";
            let (database, payload) = v2_history_payload(uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Permissions => {
            let uri =
                format!("/v2/permissions?address={V2_PERMISSIONS_SUBJECT}&include=lineage&page_size=10");
            let (database, payload) = v2_permissions_payload(&uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, &uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::AddressNames => {
            let uri = format!("/v2/addresses/{V2_ADDRESS}/names?include=role_summary");
            let (database, payload) = v2_address_names_payload(&uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, &uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::PrimaryName => {
            let database = TestDatabase::new_migrated().await?;
            database.seed_default_ens_snapshot_selector_position().await?;
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
            let uri = format!("/v2/addresses/{V2_ADDRESS}/history?page_size=20");
            let payload = v2_conformance_get_json(&database, &uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, &uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Search => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_address_names_fixture(&database).await?;
            let uri = "/v2/search?q=alpha&namespace=ens";
            let payload = v2_conformance_get_json(&database, uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::Events => {
            let database = TestDatabase::new_migrated().await?;
            seed_v2_history_fixture(&database).await?;
            let uri = "/v2/events?name=history.eth&page_size=20";
            let payload = v2_conformance_get_json(&database, uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
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
            let uri = format!(
                "/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?include=nodes,aliases,roles,events&page_size=5"
            );
            let payload = v2_resolver_payload_for_database(&database, &uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, &uri, &payload).await?;
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
            let uri = format!("/v2/diagnostics/names/Alice.eth/{suffix}");
            let payload = request_v2_diagnostics_json(&database, &uri, StatusCode::OK).await?;
            assert_v2_as_of_token_fixpoint(&database, route, &uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
        V2SuccessFixture::DiagnosticsRecords => {
            let database = TestDatabase::new_with_schemas(false, true).await?;
            seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;
            let uri = "/v2/diagnostics/names/Alice.eth/records";
            let payload = request_v2_diagnostics_json(&database, uri, StatusCode::OK).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
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
            let uri = "/v2/diagnostics/names/alice.eth/execution?keys=addr:60";
            let payload = request_v2_diagnostics_json(&database, uri, StatusCode::OK).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
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
            let uri = "/v2/diagnostics/events?name=Diag.eth&page_size=10";
            let (database, payload) = v2_diag_events_payload(uri).await?;
            assert_v2_as_of_token_fixpoint(&database, route, uri, &payload).await?;
            database.cleanup().await?;
            Ok(payload)
        }
    }
}

async fn collect_v2_stale_and_conflict_error_violations(
    violations: &mut Vec<String>,
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;
    let alice_stale_at = seed_v2_conformance_snapshot_position(
        &database,
        "ethereum",
        "ethereum-mainnet",
        21_000_002,
        "0xbinding-old",
        "2026-04-17T00:00:02Z",
    )
    .await?;
    let conflict_at = v2_at_token(
        "ethereum",
        "ethereum-mainnet",
        21_000_001,
        "0xmissing-conflict",
        "2026-04-17T00:00:01Z",
    )?;

    for (route, uri, expected_code) in [
        (
            v2_conformance_route(V2SuccessFixture::Name),
            format!("/v2/names/Alice.eth?at={alice_stale_at}"),
            "stale",
        ),
        (
            v2_conformance_route(V2SuccessFixture::NameRecords),
            format!("/v2/names/Alice.eth/records?at={alice_stale_at}"),
            "stale",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Name),
            format!("/v2/names/Alice.eth?at={conflict_at}"),
            "conflict",
        ),
    ] {
        let response = v2_conformance_response(&database, V2StrictQueryMethod::Get, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, expected_code);
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    seed_v2_address_names_fixture(&database).await?;
    let resolver_conflict_at = v2_at_token(
        "ethereum",
        "ethereum-mainnet",
        21_000_001,
        "0xmissing-conflict",
        "2026-04-17T00:00:01Z",
    )?;
    bigname_storage::upsert_resolver_current_rows(
        &database.pool,
        &[resolver_current_row_with_writer_alias(
            "ethereum-mainnet",
            V2_RESOLVER_ADDRESS,
        )],
    )
    .await?;
    for (route, uri, expected_code) in [
        (
            v2_conformance_route(V2SuccessFixture::AddressNames),
            format!("/v2/addresses/{V2_ADDRESS}/names?at={conflict_at}"),
            "conflict",
        ),
        (
            v2_conformance_route(V2SuccessFixture::AddressHistory),
            format!("/v2/addresses/{V2_ADDRESS}/history?at={conflict_at}"),
            "conflict",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Search),
            format!("/v2/search?q=alpha&namespace=ens&at={conflict_at}"),
            "conflict",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Events),
            format!("/v2/events?name=history.eth&at={conflict_at}"),
            "conflict",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Resolver),
            format!("/v2/resolvers/1/{V2_RESOLVER_ADDRESS}?at={resolver_conflict_at}"),
            "conflict",
        ),
    ] {
        let response = v2_conformance_response(&database, V2StrictQueryMethod::Get, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, expected_code);
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_v2_history_fixture(&database).await?;
    let name_tree_stale_at = seed_v2_conformance_snapshot_position(
        &database,
        "ethereum",
        "ethereum-mainnet",
        79,
        "0xname4f",
        "2026-04-17T00:00:19Z",
    )
    .await?;
    for (route, uri, expected_code) in [
        (
            v2_conformance_route(V2SuccessFixture::Subnames),
            format!("/v2/names/parent.eth/subnames?at={name_tree_stale_at}"),
            "stale",
        ),
        (
            v2_conformance_route(V2SuccessFixture::NameHistory),
            format!("/v2/names/History.eth/history?at={name_tree_stale_at}"),
            "stale",
        ),
    ] {
        let response = v2_conformance_response(&database, V2StrictQueryMethod::Get, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, expected_code);
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_v2_permissions_fixture(&database).await?;
    seed_v2_permissions_mainnet_rewind_checkpoint(&database).await?;
    let permissions_stale_at = v2_permissions_mainnet_rewind_snapshot_token()?;
    for (route, uri, expected_code) in [(
        v2_conformance_route(V2SuccessFixture::Permissions),
        format!("/v2/permissions?name=Perms.eth&at={permissions_stale_at}"),
        "stale",
    )] {
        let response = v2_conformance_response(&database, V2StrictQueryMethod::Get, &uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, expected_code);
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);
    }
    database.cleanup().await?;

    Ok(())
}

async fn collect_v2_not_found_error_violations(violations: &mut Vec<String>) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_conformance_snapshot_position(
        &database,
        "ethereum",
        "ethereum-mainnet",
        21_000_003,
        "0xbinding",
        "2026-04-17T00:00:03Z",
    )
    .await?;

    for (route, uri) in [
        (
            v2_conformance_route(V2SuccessFixture::Name),
            "/v2/names/missing.eth",
        ),
        (
            v2_conformance_route(V2SuccessFixture::NameRecords),
            "/v2/names/missing.eth/records",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Subnames),
            "/v2/names/missing.eth/subnames",
        ),
        (
            v2_conformance_route(V2SuccessFixture::NameHistory),
            "/v2/names/missing.eth/history",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Resolver),
            "/v2/resolvers/1/0x00000000000000000000000000000000000000aa",
        ),
        (
            v2_conformance_route(V2SuccessFixture::Namespace),
            "/v2/namespaces/unknown",
        ),
    ] {
        let response =
            v2_conformance_response(&database, V2StrictQueryMethod::Get, uri).await?;
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, "not_found");
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);
    }

    database.cleanup().await?;
    Ok(())
}

async fn collect_v2_internal_error_violations(violations: &mut Vec<String>) -> Result<()> {
    for route in V2_CONFORMANCE_ROUTES
        .iter()
        .filter(|route| route.tier == V2RouteTier::Product)
    {
        let Some((method, uri)) = v2_internal_error_probe(route.success) else {
            continue;
        };
        let database = if route.success == V2SuccessFixture::Namespace {
            TestDatabase::new(true).await?
        } else {
            TestDatabase::new_migrated().await?
        };
        let state = database.app_state();
        state.pool.close().await;

        let response = app_router(state)
            .oneshot(v2_conformance_request(method, uri))
            .await
            .with_context(|| format!("v2 internal-error probe failed for {}", route.label))?;
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{} {uri}",
            route.label
        );
        let payload: Value = read_json(response).await?;
        assert_v2_error_envelope(route.label, &payload, "internal_error");
        collect_pipeline_vocabulary_in_error_body(route.label, &payload, violations);

        database.cleanup().await?;
    }

    Ok(())
}

fn v2_internal_error_probe(
    fixture: V2SuccessFixture,
) -> Option<(V2StrictQueryMethod, &'static str)> {
    match fixture {
        V2SuccessFixture::Lookup => Some((V2StrictQueryMethod::PostLookup, "/v2/lookup")),
        V2SuccessFixture::Status => Some((V2StrictQueryMethod::Get, "/v2/status")),
        V2SuccessFixture::Name => Some((V2StrictQueryMethod::Get, "/v2/names/alice.eth")),
        V2SuccessFixture::NameRecords => {
            Some((V2StrictQueryMethod::Get, "/v2/names/alice.eth/records"))
        }
        V2SuccessFixture::Subnames => {
            Some((V2StrictQueryMethod::Get, "/v2/names/alice.eth/subnames"))
        }
        V2SuccessFixture::NameHistory => {
            Some((V2StrictQueryMethod::Get, "/v2/names/alice.eth/history"))
        }
        V2SuccessFixture::Permissions => Some((
            V2StrictQueryMethod::Get,
            "/v2/permissions?address=0x00000000000000000000000000000000000000aa",
        )),
        V2SuccessFixture::AddressNames => Some((
            V2StrictQueryMethod::Get,
            "/v2/addresses/0x00000000000000000000000000000000000000aa/names",
        )),
        V2SuccessFixture::PrimaryName => Some((
            V2StrictQueryMethod::Get,
            "/v2/addresses/0x00000000000000000000000000000000000000aa/primary-name",
        )),
        V2SuccessFixture::AddressHistory => Some((
            V2StrictQueryMethod::Get,
            "/v2/addresses/0x00000000000000000000000000000000000000aa/history",
        )),
        V2SuccessFixture::Search => Some((V2StrictQueryMethod::Get, "/v2/search?q=alice")),
        V2SuccessFixture::Events => Some((V2StrictQueryMethod::Get, "/v2/events")),
        V2SuccessFixture::Resolver => Some((
            V2StrictQueryMethod::Get,
            "/v2/resolvers/1/0x00000000000000000000000000000000000000aa",
        )),
        V2SuccessFixture::Namespace => Some((V2StrictQueryMethod::Get, "/v2/namespaces/ens")),
        V2SuccessFixture::DiagnosticsCoverage
        | V2SuccessFixture::DiagnosticsBinding
        | V2SuccessFixture::DiagnosticsAuthority
        | V2SuccessFixture::DiagnosticsRecords
        | V2SuccessFixture::DiagnosticsExecution
        | V2SuccessFixture::DiagnosticsNamespaceManifests
        | V2SuccessFixture::DiagnosticsEvents => None,
    }
}

async fn seed_v2_conformance_snapshot_position(
    database: &TestDatabase,
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<String> {
    database
        .seed_snapshot_selector_chain_positions(&json!({
            slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": timestamp
            }
        }))
        .await?;
    v2_at_token(slot, chain_id, block_number, block_hash, timestamp)
}

fn v2_conformance_route(fixture: V2SuccessFixture) -> &'static V2ConformanceRoute {
    V2_CONFORMANCE_ROUTES
        .iter()
        .find(|route| route.success == fixture)
        .unwrap_or_else(|| panic!("v2 conformance route missing for fixture"))
}

async fn v2_conformance_response(
    database: &TestDatabase,
    method: V2StrictQueryMethod,
    uri: &str,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(v2_conformance_request(method, uri))
        .await
        .with_context(|| format!("v2 conformance request failed for {uri}"))
}

fn v2_conformance_request(method: V2StrictQueryMethod, uri: &str) -> Request<Body> {
    let mut request = Request::builder().uri(uri);
    let body = match method {
        V2StrictQueryMethod::Get => Body::empty(),
        V2StrictQueryMethod::PostLookup => {
            request = request.method("POST").header("content-type", "application/json");
            Body::from(r#"{"inputs":[{"id":"name","name":"alice.eth"}]}"#)
        }
    };
    request.body(body).expect("request must build")
}

async fn v2_conformance_name_records_verified_stale_payload() -> Result<Value> {
    v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
        None,
    )
    .await
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

const SHARED_LIST_RECORD_FIELDS: &[&str] = &[
    "name",
    "display_name",
    "namespace",
    "namehash",
    "owner",
    "registrant",
    "registration_status",
    "registered_at",
    "created_at",
    "expires_at",
];

fn data_row_named<'a>(payload: &'a Value, name: &str, label: &str) -> &'a Value {
    payload["data"]
        .as_array()
        .unwrap_or_else(|| panic!("{label} data must be an array"))
        .iter()
        .find(|row| row["name"] == json!(name))
        .unwrap_or_else(|| panic!("{label} must include {name}"))
}

fn assert_shared_record_subset(
    profile_record: &Value,
    row: &Value,
    fields: &[&str],
    label: &str,
) {
    for field in fields {
        assert_eq!(
            row.get(*field),
            profile_record.get(*field),
            "{label} field {field} must match the profile record"
        );
    }
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
        V2AsOfExpectation::Present => {
            assert_as_of_shape(route, &payload["meta"]["as_of"]);
            assert_as_of_token_shape(route, &payload["meta"]["as_of_token"]);
        }
        V2AsOfExpectation::Conditional => {
            if payload["meta"].get("as_of").is_some()
                || payload["meta"].get("as_of_token").is_some()
            {
                assert_as_of_shape(route, &payload["meta"]["as_of"]);
                assert_as_of_token_shape(route, &payload["meta"]["as_of_token"]);
            }
        }
        V2AsOfExpectation::Absent => {
            assert!(
                payload["meta"].get("as_of").is_none(),
                "{} must omit meta.as_of",
                route.label
            );
            assert!(
                payload["meta"].get("as_of_token").is_none(),
                "{} must omit meta.as_of_token",
                route.label
            );
        }
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

fn assert_v2_name_records_verified_stale_fixture(payload: &Value) {
    assert_non_empty_json(
        &payload["data"]["records"],
        "GET /v2/names/{name}/records verified stale",
        "$.data.records",
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "verified_answer_stale_for_snapshot"
        }),
        "verified-stale records fixture must exercise the product stale reason"
    );
}

fn assert_v2_error_envelope(label: &str, payload: &Value, expected_code: &str) {
    assert_object_keys(payload, &["error"], label);
    assert_object_keys(&payload["error"], &["code", "message", "details"], label);
    assert_eq!(payload["error"]["code"], json!(expected_code), "{label}");
    assert!(
        payload["error"]["message"].is_string(),
        "{label} error message must be a string"
    );
    assert!(
        payload["error"]["details"].is_object(),
        "{label} error details must be an object"
    );
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

fn assert_as_of_token_shape(route: &V2ConformanceRoute, as_of_token: &Value) {
    let token = as_of_token
        .as_str()
        .unwrap_or_else(|| panic!("{} meta.as_of_token must be a string", route.label));
    assert!(
        !token.is_empty(),
        "{} meta.as_of_token must not be empty",
        route.label
    );
    assert!(
        token.bytes().all(is_url_safe_token_byte),
        "{} meta.as_of_token must be URL-safe",
        route.label
    );
}

fn is_url_safe_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
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

fn collect_pipeline_vocabulary_in_error_body(
    label: &str,
    payload: &Value,
    violations: &mut Vec<String>,
) {
    walk_json_fields_and_strings(&payload["error"], "$.error", &mut |path, candidate| {
        for term in matched_pipeline_terms(candidate) {
            violations.push(format!(
                "{label} at {path}: {candidate:?} contains product-banned pipeline vocabulary {term:?}"
            ));
        }
    });
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

fn walk_json_fields_and_strings(
    value: &Value,
    path: &str,
    visit: &mut impl FnMut(&str, &str),
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = json_path(path, key);
                visit(&child_path, key);
                walk_json_fields_and_strings(child, &child_path, visit);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                walk_json_fields_and_strings(child, &format!("{path}[{index}]"), visit);
            }
        }
        Value::String(text) => visit(path, text),
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
    crate::v2::matched_boundary_vocabulary_terms(candidate, crate::v2::PRODUCT_PIPELINE_TERMS)
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
fn v2_pipeline_matching_uses_shared_underscore_boundaries_and_plural_suffixes() {
    assert_eq!(matched_pipeline_terms("insufficient_coverage"), vec!["coverage"]);
    assert_eq!(matched_pipeline_terms("coverage_gap"), vec!["coverage"]);
    assert_eq!(matched_pipeline_terms("coverages"), vec!["coverage"]);
    assert!(matched_pipeline_terms("discoverage").is_empty());

    let raw_fact_matches = matched_pipeline_terms("raw facts unavailable");
    assert!(raw_fact_matches.contains(&"raw_fact"));
    assert!(raw_fact_matches.contains(&"raw fact"));
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

    assert_eq!(violations.len(), 3);
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
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("pipeline vocabulary \"resources\""))
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
