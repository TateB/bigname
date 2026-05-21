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

fn openapi_parameter_names(operation: &Value) -> Vec<&str> {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .expect("OpenAPI operation must expose parameters")
        .iter()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect()
}

fn openapi_response_description<'a>(operation: &'a Value, status: &str) -> &'a str {
    operation
        .get("responses")
        .and_then(|responses| responses.get(status))
        .and_then(|response| response.get("description"))
        .and_then(Value::as_str)
        .expect("OpenAPI response must expose a description")
}

fn assert_exact_name_snapshot_parameters(
    operation: &Value,
    expected_parameter_names: &[&str],
    expected_at_description: &str,
) {
    let actual_parameter_names = openapi_parameter_names(operation);
    assert_eq!(actual_parameter_names.as_slice(), expected_parameter_names);

    let at = openapi_parameter(operation, "at");
    assert_eq!(
        at.get("description").and_then(Value::as_str),
        Some(expected_at_description)
    );
    assert_eq!(at.get("schema"), Some(&json!({ "type": "string" })));

    let chain_positions = openapi_parameter(operation, "chain_positions");
    assert_eq!(
        chain_positions.get("description").and_then(Value::as_str),
        Some(
            "Explicit exact-name snapshot selector encoded as one JSON object using ChainPositions position objects. Mutually exclusive with `at`."
        )
    );
    assert_eq!(
        chain_positions.get("schema"),
        Some(&json!({ "type": "string" }))
    );

    let consistency = openapi_parameter(operation, "consistency");
    assert_eq!(
        consistency.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["head", "safe", "finalized"],
            "default": "head",
        }))
    );
}

fn assert_view_meta_parameters(operation: &Value, expected_view: &str, expected_meta: &str) {
    let view = openapi_parameter(operation, "view");
    assert_eq!(
        view.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["compact", "full"],
            "default": expected_view,
        }))
    );

    let meta = openapi_parameter(operation, "meta");
    assert_eq!(
        meta.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["none", "summary", "full"],
            "default": expected_meta,
        }))
    );
}

fn assert_compact_only_view_meta_parameters(operation: &Value, expected_meta: &str) {
    let view = openapi_parameter(operation, "view");
    assert_eq!(
        view.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["compact"],
            "default": "compact",
        }))
    );

    let meta = openapi_parameter(operation, "meta");
    assert_eq!(
        meta.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["none", "summary", "full"],
            "default": expected_meta,
        }))
    );
}

fn assert_no_not_implemented_response(operation: &Value) {
    assert!(
        operation
            .get("responses")
            .and_then(|responses| responses.get("501"))
            .is_none()
    );
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

fn property_names(schema: &Value) -> Vec<&str> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .expect("schema must define properties")
        .keys()
        .map(String::as_str)
        .collect()
}

fn assert_schema_omits(schema: &Value, denied_fields: &[&str]) {
    let properties = property_names(schema);
    for field in denied_fields {
        assert!(
            !properties.contains(field),
            "compact schema unexpectedly exposes {field}"
        );
    }
}

const COMPACT_SCHEMA_DENYLIST: &[&str] = &[
    "logical_name_id",
    "surface_binding_id",
    "projection_version_id",
    "raw_fact_refs",
    "normalized_event_ids",
    "execution_trace_id",
    "chain_positions",
    "coverage",
];

fn assert_compact_schema_omits(document: &Value, schema_name: &str, extra_denied_fields: &[&str]) {
    let schema = openapi_schema(document, schema_name);
    assert_schema_omits(schema, COMPACT_SCHEMA_DENYLIST);
    assert_schema_omits(schema, extra_denied_fields);
}

#[test]
fn openapi_document_publishes_only_shipped_routes() {
    let document = openapi_document();
    let actual = openapi_paths(&document).keys().cloned().collect::<Vec<_>>();

    assert_eq!(
        actual,
        vec![
            "/v1/addresses/{address}/names".to_owned(),
            "/v1/addresses/{address}/names/count".to_owned(),
            "/v1/coverage/{namespace}/{name}".to_owned(),
            "/v1/events".to_owned(),
            "/v1/explain/names/{namespace}/{name}/authority-control".to_owned(),
            "/v1/explain/names/{namespace}/{name}/surface-binding".to_owned(),
            "/v1/explain/resolutions/{namespace}/{name}/execution".to_owned(),
            "/v1/history/addresses/{address}".to_owned(),
            "/v1/history/names/{namespace}/{name}".to_owned(),
            "/v1/history/resources/{resource_id}".to_owned(),
            "/v1/identity/addresses/{address}/names".to_owned(),
            "/v1/identity/addresses:names:batch".to_owned(),
            "/v1/identity/names/{name}".to_owned(),
            "/v1/identity/names:batch".to_owned(),
            "/v1/manifests/{namespace}".to_owned(),
            "/v1/names".to_owned(),
            "/v1/names/{namespace}/{name}".to_owned(),
            "/v1/names/{namespace}/{name}/children".to_owned(),
            "/v1/names/{namespace}/{name}/records".to_owned(),
            "/v1/names/{namespace}/{name}/roles".to_owned(),
            "/v1/namespaces/{namespace}".to_owned(),
            "/v1/primary-names/{address}".to_owned(),
            "/v1/resolutions/{namespace}/{name}".to_owned(),
            "/v1/resolve/{name}".to_owned(),
            "/v1/resolve/{name}/records".to_owned(),
            "/v1/resolvers/{chain_id}/{resolver_address}".to_owned(),
            "/v1/resolvers/{chain_id}/{resolver_address}/overview".to_owned(),
            "/v1/resources/lookup".to_owned(),
            "/v1/resources/{resource_id}/permissions".to_owned(),
            "/v1/roles".to_owned(),
            "/v1/status/indexing".to_owned(),
        ]
    );
    assert!(!openapi_paths(&document).contains_key("/healthz"));
    assert!(!openapi_paths(&document).contains_key("/openapi.json"));
    assert!(!openapi_paths(&document).contains_key("/docs"));
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

    let names = openapi_operation(&document, "/v1/names");
    assert_eq!(
        openapi_parameter_names(names),
        vec![
            "namespace",
            "name",
            "prefix",
            "contains",
            "contains_nocase",
            "owner",
            "account",
            "registrant",
            "resolver",
            "resolved_address",
            "relation",
            "sort",
            "order",
            "include",
            "view",
            "meta",
            "cursor",
            "page_size",
        ]
    );
    assert_eq!(
        openapi_parameter(names, "relation").get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["token_holder", "registrant", "effective_controller", "any"],
        }))
    );
    assert_eq!(
        openapi_parameter(names, "sort").get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["name", "expiry_date", "registration_date", "created_at"],
            "default": "name",
        }))
    );
    assert_compact_only_view_meta_parameters(names, "summary");
    assert_no_not_implemented_response(names);

    let address_names_count = openapi_operation(&document, "/v1/addresses/{address}/names/count");
    assert_eq!(
        openapi_parameter_names(address_names_count),
        vec![
            "address",
            "namespace",
            "relation",
            "prefix",
            "contains",
            "contains_nocase",
            "resolver",
        ]
    );
    assert_no_not_implemented_response(address_names_count);

    let address_history = openapi_operation(&document, "/v1/history/addresses/{address}");
    assert_eq!(
        openapi_parameter_names(address_history),
        vec![
            "address",
            "namespace",
            "relation",
            "scope",
            "view",
            "meta",
            "cursor",
            "page_size",
        ]
    );
    let history_scope = openapi_parameter(address_history, "scope");
    assert_eq!(
        history_scope.get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }))
    );
    assert_view_meta_parameters(address_history, "full", "summary");

    let children = openapi_operation(&document, "/v1/names/{namespace}/{name}/children");
    assert_eq!(
        openapi_parameter_names(children),
        vec![
            "namespace",
            "name",
            "surface_classes",
            "include",
            "view",
            "meta",
            "cursor",
            "page_size"
        ]
    );
    assert_view_meta_parameters(children, "compact", "summary");
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

    let exact_name_at_description = "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.";
    for exact_name_path in [
        "/v1/names/{namespace}/{name}",
        "/v1/coverage/{namespace}/{name}",
        "/v1/explain/names/{namespace}/{name}/surface-binding",
        "/v1/explain/names/{namespace}/{name}/authority-control",
    ] {
        let operation = openapi_operation(&document, exact_name_path);
        assert_exact_name_snapshot_parameters(
            operation,
            &["namespace", "name", "at", "chain_positions", "consistency"],
            exact_name_at_description,
        );
        assert_eq!(
            openapi_response_description(operation, "400"),
            "Invalid snapshot selector"
        );
        assert_eq!(
            openapi_response_description(operation, "409"),
            "Snapshot conflict or stale projection"
        );
    }

    let resolutions = openapi_operation(&document, "/v1/resolutions/{namespace}/{name}");
    assert_exact_name_snapshot_parameters(
        resolutions,
        &[
            "namespace",
            "name",
            "at",
            "chain_positions",
            "consistency",
            "mode",
            "records",
        ],
        "Point-in-time selector for the exact-name snapshot used by resolution joins. Mutually exclusive with `chain_positions`.",
    );
    assert_eq!(
        openapi_response_description(resolutions, "409"),
        "Snapshot conflict or stale projection"
    );
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

    let name_records = openapi_operation(&document, "/v1/names/{namespace}/{name}/records");
    assert_eq!(
        openapi_parameter_names(name_records),
        vec![
            "namespace",
            "name",
            "mode",
            "texts",
            "known_text_keys",
            "avatar",
            "content_hash",
            "coin_types",
            "include",
            "view",
            "meta",
        ]
    );
    assert_eq!(
        openapi_parameter(name_records, "mode").get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["auto", "declared", "verified", "both"],
            "default": "declared",
        }))
    );
    assert_eq!(
        openapi_parameter(name_records, "include").get("style"),
        Some(&json!("form"))
    );
    assert_compact_only_view_meta_parameters(name_records, "summary");
    assert_no_not_implemented_response(name_records);

    let inferred_name_records = openapi_operation(&document, "/v1/resolve/{name}/records");
    assert_eq!(
        openapi_parameter_names(inferred_name_records),
        vec![
            "name",
            "mode",
            "texts",
            "known_text_keys",
            "avatar",
            "content_hash",
            "coin_types",
            "include",
            "view",
            "meta",
        ]
    );
    assert_eq!(
        openapi_parameter(inferred_name_records, "mode").get("schema"),
        Some(&json!({
            "type": "string",
            "enum": ["auto", "declared", "verified", "both"],
            "default": "auto",
        }))
    );
    assert_eq!(
        openapi_parameter(inferred_name_records, "include").get("style"),
        Some(&json!("form"))
    );
    assert_compact_only_view_meta_parameters(inferred_name_records, "summary");
    assert_no_not_implemented_response(inferred_name_records);

    let inferred_resolutions = openapi_operation(&document, "/v1/resolve/{name}");
    assert_eq!(
        openapi_parameter_names(inferred_resolutions),
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

    let reverse_identity_batch_input = openapi_schema(&document, "ReverseIdentityBatchInput");
    assert_eq!(
        reverse_identity_batch_input.pointer("/properties/inputs/items/properties/page_cursor/type"),
        Some(&json!(["string", "null"]))
    );

    let events = openapi_operation(&document, "/v1/events");
    assert_eq!(
        openapi_parameter_names(events),
        vec![
            "namespace",
            "name",
            "address",
            "resource",
            "resource_id",
            "type",
            "relation",
            "from_block",
            "to_block",
            "view",
            "meta",
            "cursor",
            "page_size",
        ]
    );
    assert_compact_only_view_meta_parameters(events, "summary");
    assert_no_not_implemented_response(events);

    let roles = openapi_operation(&document, "/v1/roles");
    assert_eq!(
        openapi_parameter_names(roles),
        vec![
            "account",
            "resource_id",
            "namespace",
            "name",
            "role_bitmap",
            "view",
            "meta",
            "cursor",
            "page_size",
        ]
    );
    assert_compact_only_view_meta_parameters(roles, "summary");
    assert_no_not_implemented_response(roles);

    let name_roles = openapi_operation(&document, "/v1/names/{namespace}/{name}/roles");
    assert_eq!(
        openapi_parameter_names(name_roles),
        vec![
            "namespace",
            "name",
            "account",
            "role_bitmap",
            "view",
            "meta",
            "cursor",
            "page_size",
        ]
    );
    assert_compact_only_view_meta_parameters(name_roles, "summary");
    assert_no_not_implemented_response(name_roles);

    let resource_lookup = openapi_operation(&document, "/v1/resources/lookup");
    assert_eq!(
        openapi_parameter_names(resource_lookup),
        vec!["namespace", "name", "view", "meta"]
    );
    assert_eq!(
        openapi_parameter(resource_lookup, "namespace").get("required"),
        Some(&json!(true))
    );
    assert_eq!(
        openapi_parameter(resource_lookup, "name").get("required"),
        Some(&json!(true))
    );
    assert_compact_only_view_meta_parameters(resource_lookup, "summary");
    assert_no_not_implemented_response(resource_lookup);

    let resolver_overview = openapi_operation(
        &document,
        "/v1/resolvers/{chain_id}/{resolver_address}/overview",
    );
    assert_eq!(
        openapi_parameter_names(resolver_overview),
        vec!["chain_id", "resolver_address", "include", "view", "meta"]
    );
    assert_view_meta_parameters(resolver_overview, "compact", "summary");
    assert_no_not_implemented_response(resolver_overview);

    let resolution_execution = openapi_operation(
        &document,
        "/v1/explain/resolutions/{namespace}/{name}/execution",
    );
    assert_eq!(
        openapi_parameter_names(resolution_execution),
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

    let compact_names = openapi_schema(&document, "CompactNamesResponse");
    assert_eq!(required_fields(compact_names), vec!["data", "page"]);
    assert_eq!(
        compact_names
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("data"))
            .and_then(|data| data.get("items")),
        Some(&json!({ "$ref": "#/components/schemas/CompactDomainSummary" }))
    );
    assert_eq!(
        compact_names
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("meta")),
        Some(&json!({ "$ref": "#/components/schemas/CompactMeta" }))
    );

    let compact_roles = openapi_schema(&document, "CompactRolesResponse");
    assert_eq!(required_fields(compact_roles), vec!["data", "page"]);
    assert_eq!(
        compact_roles
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("data"))
            .and_then(|data| data.get("items")),
        Some(&json!({ "$ref": "#/components/schemas/RoleRow" }))
    );

    let resource_lookup = openapi_schema(&document, "ResourceLookupResponse");
    assert_eq!(required_fields(resource_lookup), vec!["data"]);
    assert_eq!(
        resource_lookup
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("data")),
        Some(&json!({ "$ref": "#/components/schemas/ResourceLookupData" }))
    );

    assert_compact_schema_omits(&document, "CompactDomainSummary", &["resource_id", "provenance"]);
    assert_compact_schema_omits(
        &document,
        "CompactRecordSummary",
        &[
            "resource_id",
            "record_version_boundary",
            "explicit_gaps",
            "unsupported_families",
            "provenance",
        ],
    );
    assert_compact_schema_omits(
        &document,
        "CompactHistoryEvent",
        &["normalized_event_id", "provenance"],
    );
    assert_compact_schema_omits(&document, "ResourceLookupData", &["namehash"]);
    assert_compact_schema_omits(&document, "RoleRow", &["provenance"]);

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
    let checked_in =
        fs::read_to_string(&artifact_path).expect("checked-in OpenAPI artifact must exist");
    let rendered = render_openapi_document();

    assert_eq!(checked_in, rendered);

    let checked_in: Value =
        serde_json::from_str(&checked_in).expect("checked-in OpenAPI artifact must be valid JSON");
    assert!(!openapi_paths(&checked_in).contains_key("/healthz"));
    assert!(!openapi_paths(&checked_in).contains_key("/openapi.json"));
    assert!(!openapi_paths(&checked_in).contains_key("/docs"));
}

#[tokio::test]
async fn openapi_json_route_serves_generated_contract() -> Result<()> {
    let response = app_router(openapi_docs_test_state())
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload, openapi_document());

    Ok(())
}

#[tokio::test]
async fn compact_only_routes_keep_full_view_compatibility_rejection() -> Result<()> {
    for (uri, message) in [
        (
            "/v1/names?view=full",
            "view=full is reserved for a later compact names implementation",
        ),
        (
            "/v1/names/ens/alice.eth/records?view=full",
            "view=full is not supported for compact name records",
        ),
        (
            "/v1/resolve/alice.eth/records?view=full",
            "view=full is not supported for compact name records",
        ),
        (
            "/v1/names/ens/alice.eth/roles?view=full",
            "view=full is not supported for compact name roles",
        ),
        (
            "/v1/roles?view=full&account=0x0000000000000000000000000000000000000001",
            "view=full is not supported for compact roles",
        ),
        (
            "/v1/resources/lookup?namespace=ens&name=alice.eth&view=full",
            "view=full is not supported for resource lookup",
        ),
        (
            "/v1/events?view=full",
            "view=full is reserved for /v1/events until the full event shape is documented",
        ),
    ] {
        let response = app_router(openapi_docs_test_state())
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let payload: Value = read_json(response).await?;
        assert_eq!(
            payload.get("error").and_then(|error| error.get("code")),
            Some(&json!("invalid_input")),
            "{uri}"
        );
        assert_eq!(
            payload.get("error").and_then(|error| error.get("message")),
            Some(&json!(message)),
            "{uri}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn openapi_docs_route_serves_viewer() -> Result<()> {
    let response = app_router(openapi_docs_test_state())
        .oneshot(
            Request::builder()
                .uri("/docs")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read OpenAPI docs body")?;
    let body = String::from_utf8(body.to_vec()).context("OpenAPI docs body must be UTF-8")?;

    assert!(content_type.starts_with("text/html"));
    assert!(body.contains("bigname API docs"));
    assert!(body.contains("/openapi.json"));
    assert!(body.contains("Try request"));
    assert!(body.contains("Response headers"));
    assert!(body.contains("performance.now()"));
    assert!(body.contains("0x8e8Db5CcEF88cca9d624701Db544989C996E3216"));
    assert!(body.contains("taytems.eth"));

    Ok(())
}

fn openapi_docs_test_state() -> AppState {
    AppState {
        phase: "test",
        pool: PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
            .expect("OpenAPI helper route tests only need a lazily parsed pool"),
        chain_rpc_urls: bigname_execution::ChainRpcUrls::default(),
    }
}
