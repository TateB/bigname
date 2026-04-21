impl ApiRouteDefinition {
    fn register(self, router: Router<AppState>) -> Router<AppState> {
        match self.id {
            ApiRouteId::Health => router.route(self.path, get(health)),
            ApiRouteId::AddressNames => router.route(self.path, get(address_names)),
            ApiRouteId::AddressHistory => router.route(self.path, get(address_history)),
            ApiRouteId::PrimaryNames => router.route(self.path, get(primary_names)),
            ApiRouteId::Coverage => router.route(self.path, get(coverage_current)),
            ApiRouteId::ExplainSurfaceBinding => {
                router.route(self.path, get(explain_surface_binding_current))
            }
            ApiRouteId::ExplainAuthorityControl => {
                router.route(self.path, get(explain_authority_control_current))
            }
            ApiRouteId::ExplainResolutionExecution => {
                router.route(self.path, get(explain_resolution_execution_current))
            }
            ApiRouteId::NamespaceMetadata => router.route(self.path, get(namespace_metadata)),
            ApiRouteId::NameChildren => router.route(self.path, get(name_children)),
            ApiRouteId::NameCurrent => router.route(self.path, get(name_current)),
            ApiRouteId::ResolveCurrent => router.route(self.path, get(resolve_current)),
            ApiRouteId::ResolutionCurrent => router.route(self.path, get(resolution_current)),
            ApiRouteId::ResolverCurrent => router.route(self.path, get(resolver_current)),
            ApiRouteId::NameHistory => router.route(self.path, get(name_history)),
            ApiRouteId::ResourceHistory => router.route(self.path, get(resource_history)),
            ApiRouteId::ResourcePermissions => router.route(self.path, get(resource_permissions)),
            ApiRouteId::NamespaceManifests => router.route(self.path, get(namespace_manifests)),
        }
    }

    fn openapi_path_item(self) -> Option<JsonValue> {
        self.published_in_contract
            .then(|| json!({ "get": self.id.openapi_operation() }))
    }
}

impl ApiRouteId {
    fn operation_id(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::AddressNames => "address_names",
            Self::AddressHistory => "address_history",
            Self::PrimaryNames => "primary_names",
            Self::Coverage => "coverage_current",
            Self::ExplainSurfaceBinding => "explain_surface_binding_current",
            Self::ExplainAuthorityControl => "explain_authority_control_current",
            Self::ExplainResolutionExecution => "explain_resolution_execution_current",
            Self::NamespaceMetadata => "namespace_metadata",
            Self::NameChildren => "name_children",
            Self::NameCurrent => "name_current",
            Self::ResolveCurrent => "resolve_current",
            Self::ResolutionCurrent => "resolution_current",
            Self::ResolverCurrent => "resolver_current",
            Self::NameHistory => "name_history",
            Self::ResourceHistory => "resource_history",
            Self::ResourcePermissions => "resource_permissions",
            Self::NamespaceManifests => "namespace_manifests",
        }
    }

    fn openapi_operation(self) -> JsonValue {
        match self {
            Self::Health => openapi_json_get_operation(
                self.operation_id(),
                "Health check",
                "Health",
                Vec::new(),
                "HealthResponse",
                false,
                false,
            ),
            Self::AddressNames => openapi_json_get_operation(
                self.operation_id(),
                "Address-to-surface collection",
                "Collections",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    dedupe_by_query_parameter(),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `role_summary` is the only shipped expansion.",
                        json!({
                            "type": "string",
                            "enum": ["role_summary"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::AddressHistory => openapi_json_get_operation(
                self.operation_id(),
                "Address activity across related surfaces and resources",
                "History",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::PrimaryNames => openapi_json_get_operation(
                self.operation_id(),
                "Claimed and verified primary-name answer",
                "Resolution",
                vec![
                    address_path_parameter(),
                    required_namespace_query_parameter(),
                    required_coin_type_query_parameter(),
                    primary_name_mode_query_parameter(),
                ],
                "PrimaryNameResponse",
                true,
                true,
            ),
            Self::Coverage => openapi_json_get_operation(
                self.operation_id(),
                "Single-name coverage and explain details",
                "Coverage",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ExplainSurfaceBinding => openapi_json_get_operation(
                self.operation_id(),
                "Current surface-binding explain view for one exact name",
                "Explain",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ExplainAuthorityControl => openapi_json_get_operation(
                self.operation_id(),
                "Current authority/control explain view for one exact name",
                "Explain",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ExplainResolutionExecution => openapi_json_get_operation(
                self.operation_id(),
                "Persisted verified execution explain for one exact-name resolution request",
                "Explain",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    required_csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required for the persisted execution explain lookup.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::NamespaceMetadata => openapi_json_get_operation(
                self.operation_id(),
                "Namespace metadata and support status",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceMetadataResponse",
                false,
                true,
            ),
            Self::NameChildren => openapi_json_get_operation(
                self.operation_id(),
                "Declared child collection by default",
                "Collections",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    csv_query_parameter(
                        "surface_classes",
                        "Requested child surface classes. Only `declared` is currently supported.",
                        json!({
                            "type": "string",
                            "default": "declared",
                        }),
                    ),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `counts` includes `declared_state.subname_count`.",
                        json!({
                            "type": "string",
                            "enum": ["counts"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::NameCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Exact name lookup",
                "Names",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ResolveCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Namespace-inferred resolution topology, inventory, and verified reads",
                "Resolution",
                vec![
                    name_path_parameter(),
                    resolution_mode_query_parameter(),
                    csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required when `mode` is `verified` or `both`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::ResolutionCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolution topology, inventory, and verified reads",
                "Resolution",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    resolution_mode_query_parameter(),
                    csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required when `mode` is `verified` or `both`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::ResolverCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolver overview",
                "Resolvers",
                vec![chain_id_path_parameter(), resolver_address_path_parameter()],
                "ResolverResponse",
                false,
                true,
            ),
            Self::NameHistory => openapi_json_get_operation(
                self.operation_id(),
                "Surface or combined history",
                "History",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourceHistory => openapi_json_get_operation(
                self.operation_id(),
                "Resource history",
                "History",
                vec![
                    resource_id_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourcePermissions => openapi_json_get_operation(
                self.operation_id(),
                "Resource-centric effective permissions",
                "Collections",
                vec![
                    resource_id_path_parameter(),
                    query_parameter(
                        "subject",
                        "Optional subject filter for the current effective permissions rows.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    query_parameter(
                        "scope",
                        "Optional scope filter. Accepts `root`, `registry`, `resource`, `resolver:{chain_id}:{resolver_address}`, `record_manager:{chain_id}:{manager_address}`, `migration_derived:{resource_id}`, or `transport_derived:{transport}`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::NamespaceManifests => openapi_json_get_operation(
                self.operation_id(),
                "Active manifest versions and capabilities",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceManifestsResponse",
                false,
                true,
            ),
        }
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let state = AppState {
        phase: bigname_domain::bootstrap_phase(),
        pool,
    };
    let router = app_router(state);
    let listener = tokio::net::TcpListener::bind(args.bind_addr)
        .await
        .context("failed to bind the API listener")?;

    info!(
        service = "api",
        bind_addr = %args.bind_addr,
        phase = bigname_domain::bootstrap_phase(),
        "API booted"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal("api"))
        .await
        .context("API server exited unexpectedly")
}

fn app_router(state: AppState) -> Router {
    API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .fold(Router::new(), |router, route| route.register(router))
        .with_state(state)
}

fn render_openapi_document() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&openapi_document()).expect("OpenAPI document must render");
    rendered.push('\n');
    rendered
}

fn openapi_document() -> JsonValue {
    let mut paths = JsonMap::new();
    for route in API_ROUTE_DEFINITIONS {
        if let Some(path_item) = route.openapi_path_item() {
            paths.insert(route.path.to_owned(), path_item);
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "bigname API v1",
            "version": "phase-7",
            "description": "Machine-readable publication of the currently shipped public API surface derived from apps/api/src/main.rs.",
        },
        "paths": JsonValue::Object(paths),
        "components": openapi_components(),
    })
}

fn openapi_components() -> JsonValue {
    json!({
        "schemas": {
            "JsonObject": json_object_schema(),
            "NullValue": json!({ "type": "null" }),
            "Consistency": json!({
                "type": "string",
                "enum": ["head", "safe", "finalized"],
            }),
            "Provenance": json!({
                "type": "object",
                "required": [
                    "normalized_event_ids",
                    "raw_fact_refs",
                    "manifest_versions",
                    "execution_trace_id",
                    "derivation_kind",
                ],
                "properties": {
                    "normalized_event_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "raw_fact_refs": {
                        "type": "array",
                        "items": {},
                    },
                    "manifest_versions": {
                        "type": "array",
                        "items": {},
                    },
                    "execution_trace_id": {
                        "type": ["string", "null"],
                    },
                    "derivation_kind": {
                        "type": "string",
                    },
                },
            }),
            "CoverageResponse": json!({
                "type": "object",
                "required": [
                    "status",
                    "exhaustiveness",
                    "source_classes_considered",
                    "enumeration_basis",
                    "unsupported_reason",
                ],
                "properties": {
                    "status": { "type": "string" },
                    "exhaustiveness": { "type": "string" },
                    "source_classes_considered": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "enumeration_basis": { "type": "string" },
                    "unsupported_reason": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ChainPositionResponse": json!({
                "type": "object",
                "required": ["chain_id", "block_number", "block_hash", "timestamp"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "block_number": { "type": "integer" },
                    "block_hash": { "type": "string" },
                    "timestamp": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
            }),
            "ChainPositions": json!({
                "type": "object",
                "additionalProperties": schema_ref("ChainPositionResponse"),
            }),
            "HistoryPageResponse": json!({
                "type": "object",
                "required": ["cursor", "next_cursor", "page_size", "sort"],
                "properties": {
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "page_size": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "sort": { "type": "string" },
                },
            }),
            "ExactNameData": json!({
                "type": "object",
                "required": [
                    "logical_name_id",
                    "namespace",
                    "normalized_name",
                    "canonical_display_name",
                    "namehash",
                    "resource_id",
                    "token_lineage_id",
                    "binding_kind",
                ],
                "properties": {
                    "logical_name_id": { "type": "string" },
                    "namespace": { "type": "string" },
                    "normalized_name": { "type": "string" },
                    "canonical_display_name": { "type": "string" },
                    "namehash": { "type": "string" },
                    "resource_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "token_lineage_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "binding_kind": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ResolverData": json!({
                "type": "object",
                "required": ["chain_id", "resolver_address"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "resolver_address": { "type": "string" },
                },
            }),
            "PrimaryNameData": json!({
                "type": "object",
                "required": ["address", "namespace", "coin_type"],
                "properties": {
                    "address": { "type": "string" },
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                    "coin_type": { "type": "string" },
                },
            }),
            "PrimaryNameClaimedResult": primary_name_claimed_result_schema(),
            "PrimaryNameDeclaredState": json!({
                "type": "object",
                "required": ["claimed_primary_name"],
                "properties": {
                    "claimed_primary_name": schema_ref("PrimaryNameClaimedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedState": json!({
                "type": "object",
                "required": ["verified_primary_name"],
                "properties": {
                    "verified_primary_name": schema_ref("PrimaryNameVerifiedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedResult": primary_name_verified_result_schema(),
            "PrimaryNameVerifiedResultProvenance": primary_name_verified_result_provenance_schema(),
            "ExactNameResponse": declared_response_schema(
                schema_ref("ExactNameData"),
                schema_ref("JsonObject"),
            ),
            "ResolverResponse": declared_response_schema(
                schema_ref("ResolverData"),
                schema_ref("JsonObject"),
            ),
            "ResolutionResponse": mixed_response_schema(schema_ref("ExactNameData")),
            "PrimaryNameResponse": primary_name_response_schema(),
            "CollectionResponse": paginated_declared_response_schema(
                json!({
                    "type": "array",
                    "items": schema_ref("JsonObject"),
                }),
                schema_ref("JsonObject"),
            ),
            "NamespaceData": json!({
                "type": "object",
                "required": ["namespace"],
                "properties": {
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                },
            }),
            "NamespaceMetadataDeclaredState": json!({
                "type": "object",
                "required": [
                    "active_manifest_count",
                    "active_source_families",
                    "chains",
                    "normalizer_versions",
                ],
                "properties": {
                    "active_manifest_count": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "active_source_families": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "chains": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "normalizer_versions": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                },
            }),
            "NamespaceMetadataResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceMetadataDeclaredState"),
            ),
            "CapabilityFlag": json!({
                "type": "object",
                "required": ["status", "notes"],
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["unsupported", "shadow", "supported"],
                    },
                    "notes": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "NamespaceManifestEntry": json!({
                "type": "object",
                "required": [
                    "manifest_version",
                    "source_family",
                    "chain",
                    "deployment_epoch",
                    "normalizer_version",
                    "capability_flags",
                ],
                "properties": {
                    "manifest_version": {
                        "type": "integer",
                        "minimum": 1,
                    },
                    "source_family": { "type": "string" },
                    "chain": { "type": "string" },
                    "deployment_epoch": { "type": "string" },
                    "normalizer_version": { "type": "string" },
                    "capability_flags": {
                        "type": "object",
                        "additionalProperties": schema_ref("CapabilityFlag"),
                    },
                },
            }),
            "NamespaceManifestsDeclaredState": json!({
                "type": "object",
                "required": ["manifests"],
                "properties": {
                    "manifests": {
                        "type": "array",
                        "items": schema_ref("NamespaceManifestEntry"),
                    },
                },
            }),
            "NamespaceManifestsResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceManifestsDeclaredState"),
            ),
            "HealthResponse": json!({
                "type": "object",
                "required": ["service", "phase", "status"],
                "properties": {
                    "service": { "type": "string" },
                    "phase": { "type": "string" },
                    "status": { "type": "string" },
                },
            }),
            "ErrorBody": json!({
                "type": "object",
                "required": ["code", "message", "details"],
                "properties": {
                    "code": { "type": "string" },
                    "message": { "type": "string" },
                    "details": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                    },
                },
            }),
            "ErrorResponse": json!({
                "type": "object",
                "required": ["error"],
                "properties": {
                    "error": schema_ref("ErrorBody"),
                },
            }),
        },
    })
}

fn declared_response_schema(data_schema: JsonValue, declared_state_schema: JsonValue) -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": data_schema,
            "declared_state": declared_state_schema,
            "verified_state": schema_ref("NullValue"),
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

fn mixed_response_schema(data_schema: JsonValue) -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": data_schema,
            "declared_state": {
                "type": ["object", "null"],
                "additionalProperties": true,
            },
            "verified_state": {
                "type": ["object", "null"],
                "additionalProperties": true,
            },
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

fn primary_name_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": schema_ref("PrimaryNameData"),
            "declared_state": nullable_ref_schema("PrimaryNameDeclaredState"),
            "verified_state": nullable_ref_schema("PrimaryNameVerifiedState"),
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

fn primary_name_claimed_result_schema() -> JsonValue {
    json!({
        "oneOf": [
            json!({
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
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "unsupported_reason": {
                        "type": "string",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
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
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
        ],
    })
}

fn primary_name_verified_result_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {
                "type": "string",
            },
            "provenance": schema_ref("PrimaryNameVerifiedResultProvenance"),
        },
        "additionalProperties": true,
    })
}

fn primary_name_verified_result_provenance_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["manifest_versions", "execution_trace_id"],
        "properties": {
            "manifest_versions": {
                "type": "array",
                "items": {},
            },
            "execution_trace_id": {
                "type": "string",
            },
        },
        "additionalProperties": false,
    })
}

fn paginated_declared_response_schema(
    data_schema: JsonValue,
    declared_state_schema: JsonValue,
) -> JsonValue {
    let mut schema = declared_response_schema(data_schema, declared_state_schema);
    let object = schema
        .as_object_mut()
        .expect("declared response schema must be an object");
    object
        .get_mut("required")
        .and_then(JsonValue::as_array_mut)
        .expect("declared response schema must define required fields")
        .push(JsonValue::String("page".to_owned()));
    object
        .get_mut("properties")
        .and_then(JsonValue::as_object_mut)
        .expect("declared response schema must define properties")
        .insert("page".to_owned(), schema_ref("HistoryPageResponse"));
    schema
}

fn openapi_json_get_operation(
    operation_id: &'static str,
    summary: &'static str,
    tag: &'static str,
    parameters: Vec<JsonValue>,
    success_schema: &'static str,
    include_bad_request: bool,
    include_not_found: bool,
) -> JsonValue {
    let mut responses = JsonMap::new();
    responses.insert(
        "200".to_owned(),
        json_response("Successful response", success_schema),
    );
    if include_bad_request {
        responses.insert(
            "400".to_owned(),
            json_response("Invalid request", "ErrorResponse"),
        );
    }
    if include_not_found {
        responses.insert(
            "404".to_owned(),
            json_response("Requested resource was not found", "ErrorResponse"),
        );
    }
    responses.insert(
        "500".to_owned(),
        json_response("Internal error", "ErrorResponse"),
    );

    json!({
        "operationId": operation_id,
        "summary": summary,
        "tags": [tag],
        "parameters": parameters,
        "responses": JsonValue::Object(responses),
    })
}

fn json_response(description: &'static str, schema_name: &'static str) -> JsonValue {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": schema_ref(schema_name),
            },
        },
    })
}

fn schema_ref(schema_name: &str) -> JsonValue {
    json!({
        "$ref": format!("#/components/schemas/{schema_name}"),
    })
}

fn nullable_ref_schema(schema_name: &str) -> JsonValue {
    json!({
        "anyOf": [
            schema_ref(schema_name),
            schema_ref("NullValue"),
        ],
    })
}

fn json_object_schema() -> JsonValue {
    json!({
        "type": "object",
        "additionalProperties": true,
    })
}

fn path_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "description": description.into(),
        "schema": schema,
    })
}

fn query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "query",
        "required": false,
        "description": description.into(),
        "schema": schema,
    })
}

fn required_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = query_parameter(name, description, schema);
    parameter
        .as_object_mut()
        .expect("required query parameter helper must create an object")
        .insert("required".to_owned(), JsonValue::Bool(true));
    parameter
}

fn csv_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = query_parameter(name, description, schema);
    let object = parameter
        .as_object_mut()
        .expect("query parameter helper must create an object");
    object.insert("style".to_owned(), JsonValue::String("form".to_owned()));
    object.insert("explode".to_owned(), JsonValue::Bool(false));
    parameter
}

fn required_csv_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = csv_query_parameter(name, description, schema);
    parameter
        .as_object_mut()
        .expect("required CSV query parameter helper must create an object")
        .insert("required".to_owned(), JsonValue::Bool(true));
    parameter
}

fn namespace_path_parameter() -> JsonValue {
    path_parameter(
        "namespace",
        "Supported namespace identifier.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

fn name_path_parameter() -> JsonValue {
    path_parameter(
        "name",
        "Normalized name within the namespace.",
        json!({
            "type": "string",
        }),
    )
}

fn address_path_parameter() -> JsonValue {
    path_parameter(
        "address",
        "Address anchor for the collection or history read. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

fn resource_id_path_parameter() -> JsonValue {
    path_parameter(
        "resource_id",
        "Resource identifier anchor.",
        json!({
            "type": "string",
            "format": "uuid",
        }),
    )
}

fn chain_id_path_parameter() -> JsonValue {
    path_parameter(
        "chain_id",
        "Resolver chain identifier.",
        json!({
            "type": "string",
        }),
    )
}

fn resolver_address_path_parameter() -> JsonValue {
    path_parameter(
        "resolver_address",
        "Resolver address anchor. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

fn namespace_query_parameter() -> JsonValue {
    query_parameter(
        "namespace",
        "Optional namespace filter.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

fn required_namespace_query_parameter() -> JsonValue {
    required_query_parameter(
        "namespace",
        "Required namespace identifier for the requested primary-name tuple.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

fn relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional relation facet filter.",
        json!({
            "type": "string",
            "enum": ["registrant", "token_holder", "effective_controller"],
        }),
    )
}

fn dedupe_by_query_parameter() -> JsonValue {
    query_parameter(
        "dedupe_by",
        "Current collection dedupe basis.",
        json!({
            "type": "string",
            "enum": ["surface", "resource"],
            "default": "surface",
        }),
    )
}

fn history_scope_query_parameter() -> JsonValue {
    query_parameter(
        "scope",
        "History scope selector.",
        json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }),
    )
}

fn resolution_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Resolution read mode.",
        json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }),
    )
}

fn primary_name_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Primary-name read mode.",
        json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }),
    )
}

fn required_coin_type_query_parameter() -> JsonValue {
    required_query_parameter(
        "coin_type",
        "Required `coin_type` selector for the requested primary-name tuple.",
        json!({
            "type": "string",
            "pattern": "^[0-9]+$",
        }),
    )
}

fn cursor_query_parameter() -> JsonValue {
    query_parameter(
        "cursor",
        "Replay-stable pagination cursor.",
        json!({
            "type": "string",
        }),
    )
}

fn page_size_query_parameter() -> JsonValue {
    query_parameter(
        "page_size",
        format!("Optional page size. When supplied it must be between 1 and {MAX_PAGE_SIZE}."),
        json!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_PAGE_SIZE,
        }),
    )
}
