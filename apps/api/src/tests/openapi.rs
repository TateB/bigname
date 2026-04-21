fn openapi_paths(document: &Value) -> &serde_json::Map<String, Value> {
    document
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI document must expose paths")
}

fn openapi_operation<'a>(document: &'a Value, path: &str) -> &'a Value {
    openapi_paths(document)
        .get(path)
        .and_then(|path_item| path_item.get("get"))
        .expect("OpenAPI path must expose a GET operation")
}

fn openapi_parameter<'a>(operation: &'a Value, name: &str) -> &'a Value {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .expect("OpenAPI operation must expose parameters")
        .iter()
        .find(|parameter| parameter.get("name") == Some(&json!(name)))
        .expect("expected parameter to exist")
}

fn openapi_schema<'a>(document: &'a Value, name: &str) -> &'a Value {
    document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .and_then(|schemas| schemas.get(name))
        .expect("expected OpenAPI schema to exist")
}

fn required_fields(schema: &Value) -> Vec<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .expect("schema must define required fields")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("required field names must be strings")
        })
        .collect()
}

#[test]
fn openapi_document_publishes_only_shipped_routes() {
    let document = openapi_document();
    let actual = openapi_paths(&document).keys().cloned().collect::<Vec<_>>();

    assert_eq!(
        actual,
        vec![
            "/v1/addresses/{address}/names".to_owned(),
            "/v1/coverage/{namespace}/{name}".to_owned(),
            "/v1/explain/names/{namespace}/{name}/authority-control".to_owned(),
            "/v1/explain/names/{namespace}/{name}/surface-binding".to_owned(),
            "/v1/explain/resolutions/{namespace}/{name}/execution".to_owned(),
            "/v1/history/addresses/{address}".to_owned(),
            "/v1/history/names/{namespace}/{name}".to_owned(),
            "/v1/history/resources/{resource_id}".to_owned(),
            "/v1/manifests/{namespace}".to_owned(),
            "/v1/names/{namespace}/{name}".to_owned(),
            "/v1/names/{namespace}/{name}/children".to_owned(),
            "/v1/namespaces/{namespace}".to_owned(),
            "/v1/primary-names/{address}".to_owned(),
            "/v1/resolutions/{namespace}/{name}".to_owned(),
            "/v1/resolve/{name}".to_owned(),
            "/v1/resolvers/{chain_id}/{resolver_address}".to_owned(),
            "/v1/resources/{resource_id}/permissions".to_owned(),
        ]
    );
    assert!(!openapi_paths(&document).contains_key("/healthz"));
}

#[test]
fn openapi_document_freezes_query_params_and_shared_envelopes() {
    let document = openapi_document();

    let address_names = openapi_operation(&document, "/v1/addresses/{address}/names");
    let dedupe_by = openapi_parameter(address_names, "dedupe_by");
    assert_eq!(
        dedupe_by.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["surface", "resource"],
            "default": "surface",
        }))
    );
    let page_size = openapi_parameter(address_names, "page_size");
    assert_eq!(
        page_size.get("schema"),
        Some(&json!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_PAGE_SIZE,
        }))
    );

    let address_history = openapi_operation(&document, "/v1/history/addresses/{address}");
    let history_scope = openapi_parameter(address_history, "scope");
    assert_eq!(
        history_scope.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }))
    );

    let children = openapi_operation(&document, "/v1/names/{namespace}/{name}/children");
    let surface_classes = openapi_parameter(children, "surface_classes");
    assert_eq!(
        surface_classes.get("schema"),
        Some(&json!({
            "type": "string",
            "default": "declared",
        }))
    );
    assert_eq!(surface_classes.get("style"), Some(&json!("form")));
    assert_eq!(surface_classes.get("explode"), Some(&json!(false)));

    let resolutions = openapi_operation(&document, "/v1/resolutions/{namespace}/{name}");
    let mode = openapi_parameter(resolutions, "mode");
    assert_eq!(
        mode.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }))
    );
    let records = openapi_parameter(resolutions, "records");
    assert_eq!(records.get("style"), Some(&json!("form")));
    assert_eq!(records.get("explode"), Some(&json!(false)));

    let inferred_resolutions = openapi_operation(&document, "/v1/resolve/{name}");
    let inferred_resolution_parameters = inferred_resolutions
        .get("parameters")
        .and_then(Value::as_array)
        .expect("namespace-inferred resolution must expose parameters");
    let inferred_resolution_parameter_names = inferred_resolution_parameters
        .iter()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        inferred_resolution_parameter_names,
        vec!["name", "mode", "records"]
    );
    let inferred_mode = openapi_parameter(inferred_resolutions, "mode");
    assert_eq!(inferred_mode.get("schema"), mode.get("schema"));
    let inferred_records = openapi_parameter(inferred_resolutions, "records");
    assert_eq!(inferred_records.get("style"), Some(&json!("form")));
    assert_eq!(inferred_records.get("explode"), Some(&json!(false)));
    assert_eq!(
        inferred_resolutions
            .get("responses")
            .and_then(|responses| responses.get("200"))
            .and_then(|response| response.get("content"))
            .and_then(|content| content.get("application/json"))
            .and_then(|content_type| content_type.get("schema")),
        Some(&json!({ "$ref": "#/components/schemas/ResolutionResponse" }))
    );

    let resolution_execution = openapi_operation(
        &document,
        "/v1/explain/resolutions/{namespace}/{name}/execution",
    );
    let resolution_execution_parameters = resolution_execution
        .get("parameters")
        .and_then(Value::as_array)
        .expect("resolution execution explain must expose parameters");
    let resolution_execution_parameter_names = resolution_execution_parameters
        .iter()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        resolution_execution_parameter_names,
        vec!["namespace", "name", "records"]
    );
    let resolution_execution_records = openapi_parameter(resolution_execution, "records");
    assert_eq!(
        resolution_execution_records.get("schema"),
        Some(&json!({ "type": "string" }))
    );
    assert_eq!(
        resolution_execution_records.get("required"),
        Some(&json!(true))
    );
    assert_eq!(
        resolution_execution_records.get("style"),
        Some(&json!("form"))
    );
    assert_eq!(
        resolution_execution_records.get("explode"),
        Some(&json!(false))
    );

    let primary_names = openapi_operation(&document, "/v1/primary-names/{address}");
    let primary_namespace = openapi_parameter(primary_names, "namespace");
    assert_eq!(primary_namespace.get("required"), Some(&json!(true)));
    assert_eq!(
        primary_namespace.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["ens", "basenames"],
        }))
    );
    let primary_coin_type = openapi_parameter(primary_names, "coin_type");
    assert_eq!(primary_coin_type.get("required"), Some(&json!(true)));
    assert_eq!(
        primary_coin_type.get("schema"),
        Some(&json!({
            "type": "string",
            "pattern": "^[0-9]+$",
        }))
    );
    let primary_mode = openapi_parameter(primary_names, "mode");
    assert_eq!(
        primary_mode.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }))
    );

    let exact_name = openapi_schema(&document, "ExactNameResponse");
    assert_eq!(
        required_fields(exact_name),
        vec![
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ]
    );
    assert_eq!(
        exact_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("coverage")),
        Some(&json!({ "$ref": "#/components/schemas/CoverageResponse" }))
    );

    let collection = openapi_schema(&document, "CollectionResponse");
    assert_eq!(
        required_fields(collection),
        vec![
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
            "page",
        ]
    );
    assert_eq!(
        collection
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("page")),
        Some(&json!({ "$ref": "#/components/schemas/HistoryPageResponse" }))
    );

    let resolution = openapi_schema(&document, "ResolutionResponse");
    assert_eq!(
        resolution
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("declared_state")),
        Some(&json!({
            "type": ["object", "null"],
            "additionalProperties": true,
        }))
    );
    assert_eq!(
        resolution
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_state")),
        Some(&json!({
            "type": ["object", "null"],
            "additionalProperties": true,
        }))
    );

    let primary_name = openapi_schema(&document, "PrimaryNameResponse");
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("data")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameData" }))
    );
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("declared_state")),
        Some(&json!({
            "anyOf": [
                { "$ref": "#/components/schemas/PrimaryNameDeclaredState" },
                { "$ref": "#/components/schemas/NullValue" },
            ],
        }))
    );
    assert_eq!(
        primary_name
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_state")),
        Some(&json!({
            "anyOf": [
                { "$ref": "#/components/schemas/PrimaryNameVerifiedState" },
                { "$ref": "#/components/schemas/NullValue" },
            ],
        }))
    );
    let primary_name_declared_state = openapi_schema(&document, "PrimaryNameDeclaredState");
    assert_eq!(
        required_fields(primary_name_declared_state),
        vec!["claimed_primary_name"]
    );
    assert_eq!(
        primary_name_declared_state
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("claimed_primary_name")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameClaimedResult" }))
    );
    assert_eq!(
        primary_name_declared_state.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_verified_state = openapi_schema(&document, "PrimaryNameVerifiedState");
    assert_eq!(
        required_fields(primary_name_verified_state),
        vec!["verified_primary_name"]
    );
    assert_eq!(
        primary_name_verified_state
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("verified_primary_name")),
        Some(&json!({ "$ref": "#/components/schemas/PrimaryNameVerifiedResult" }))
    );
    assert_eq!(
        primary_name_verified_state.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_verified_result = openapi_schema(&document, "PrimaryNameVerifiedResult");
    assert_eq!(
        primary_name_verified_result.get("type"),
        Some(&json!("object"))
    );
    assert_eq!(
        required_fields(primary_name_verified_result),
        vec!["status"]
    );
    assert_eq!(
        primary_name_verified_result
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("status")),
        Some(&json!({
            "type": "string",
        }))
    );
    assert_eq!(
        primary_name_verified_result
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("provenance")),
        Some(&json!({
            "$ref": "#/components/schemas/PrimaryNameVerifiedResultProvenance",
        }))
    );
    assert_eq!(
        primary_name_verified_result.get("additionalProperties"),
        Some(&json!(true))
    );

    let primary_name_verified_result_provenance =
        openapi_schema(&document, "PrimaryNameVerifiedResultProvenance");
    assert_eq!(
        required_fields(primary_name_verified_result_provenance),
        vec!["manifest_versions", "execution_trace_id"]
    );
    assert_eq!(
        primary_name_verified_result_provenance
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("manifest_versions")),
        Some(&json!({
            "type": "array",
            "items": {},
        }))
    );
    assert_eq!(
        primary_name_verified_result_provenance
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("execution_trace_id")),
        Some(&json!({
            "type": "string",
        }))
    );
    assert_eq!(
        primary_name_verified_result_provenance.get("additionalProperties"),
        Some(&json!(false))
    );

    let primary_name_claimed_result = openapi_schema(&document, "PrimaryNameClaimedResult");
    let primary_name_claimed_variants = primary_name_claimed_result
        .get("oneOf")
        .and_then(Value::as_array)
        .expect("PrimaryNameClaimedResult must define oneOf variants");
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "success",
                    },
                    "name": {
                        "type": "string",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            == &json!({
                "type": "object",
                "required": ["status", "raw_claim_name", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "invalid_name",
                    },
                    "raw_claim_name": {
                        "type": "string",
                    },
                    "provenance": {
                        "$ref": "#/components/schemas/JsonObject",
                    },
                },
                "additionalProperties": false,
            })
    }));
    assert!(primary_name_claimed_variants.iter().any(|variant| {
        variant
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| {
                properties.get("status") == Some(&json!({"type": "string", "const": "success"}))
                    && properties.contains_key("name")
            })
    }));
    assert!(primary_name_claimed_variants.iter().all(|variant| {
        let status_is_success = variant
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("status"))
            == Some(&json!({
                "type": "string",
                "const": "success",
            }));
        status_is_success
            || !variant
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| properties.contains_key("name"))
    }));

    let coverage = openapi_schema(&document, "CoverageResponse");
    assert_eq!(
        required_fields(coverage),
        vec![
            "status",
            "exhaustiveness",
            "source_classes_considered",
            "enumeration_basis",
            "unsupported_reason",
        ]
    );
}

#[test]
fn openapi_document_matches_checked_in_artifact() {
    let artifact_path = format!(
        "{}/../../docs/api-v1.openapi.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let checked_in: Value = serde_json::from_str(
        &fs::read_to_string(&artifact_path).expect("checked-in OpenAPI artifact must exist"),
    )
    .expect("checked-in OpenAPI artifact must be valid JSON");
    let rendered: Value = serde_json::from_str(&render_openapi_document())
        .expect("rendered OpenAPI document must be valid JSON");
    assert_eq!(checked_in, rendered);
}
