#[path = "routes/parameters.rs"]
mod route_parameters;

use route_parameters::*;
pub(crate) use route_parameters::{ApiParameterLocation, ApiParameterSchema, ApiRouteParameter};

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteDefinition {
    pub(crate) id: ApiRouteId,
    pub(crate) method: ApiRouteMethod,
    pub(crate) path: &'static str,
    pub(crate) contract: Option<ApiRouteContract>,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteMethod {
    Get,
    Post,
}

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteContract {
    pub(crate) operation_id: &'static str,
    pub(crate) summary: &'static str,
    pub(crate) tag: &'static str,
    pub(crate) parameters: &'static [ApiRouteParameter],
    pub(crate) request_schema: Option<&'static str>,
    pub(crate) success_schema: &'static str,
    pub(crate) errors: ApiRouteErrorResponses,
}

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteErrorResponses {
    pub(crate) include_bad_request: bool,
    pub(crate) include_not_found: bool,
    pub(crate) bad_request_description: Option<&'static str>,
    pub(crate) include_conflict: bool,
}

impl ApiRouteDefinition {
    const fn private_get(id: ApiRouteId, path: &'static str) -> Self {
        Self {
            id,
            method: ApiRouteMethod::Get,
            path,
            contract: None,
        }
    }

    const fn public_get(id: ApiRouteId, path: &'static str, contract: ApiRouteContract) -> Self {
        Self {
            id,
            method: ApiRouteMethod::Get,
            path,
            contract: Some(contract),
        }
    }

    const fn public_post(id: ApiRouteId, path: &'static str, contract: ApiRouteContract) -> Self {
        Self {
            id,
            method: ApiRouteMethod::Post,
            path,
            contract: Some(contract),
        }
    }
}

impl ApiRouteContract {
    const fn new(
        operation_id: &'static str,
        summary: &'static str,
        tag: &'static str,
        parameters: &'static [ApiRouteParameter],
        success_schema: &'static str,
        errors: ApiRouteErrorResponses,
    ) -> Self {
        Self {
            operation_id,
            summary,
            tag,
            parameters,
            request_schema: None,
            success_schema,
            errors,
        }
    }

    const fn with_request_schema(mut self, request_schema: &'static str) -> Self {
        self.request_schema = Some(request_schema);
        self
    }
}

impl ApiRouteErrorResponses {
    const fn new(include_bad_request: bool, include_not_found: bool) -> Self {
        Self {
            include_bad_request,
            include_not_found,
            bad_request_description: None,
            include_conflict: false,
        }
    }

    const fn snapshot() -> Self {
        Self {
            include_bad_request: true,
            include_not_found: true,
            bad_request_description: Some("Invalid snapshot selector"),
            include_conflict: true,
        }
    }

    const fn conflict(include_not_found: bool) -> Self {
        Self {
            include_bad_request: true,
            include_not_found,
            bad_request_description: None,
            include_conflict: true,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteId {
    Health,
    PublicStatus,
    IdentityLookup,
    Names,
    AddressNames,
    AddressHistory,
    PrimaryNames,
    Coverage,
    ExplainSurfaceBinding,
    ExplainAuthorityControl,
    ExplainResolutionExecution,
    NamespaceMetadata,
    NameChildren,
    NameCurrent,
    NameProfile,
    NameRecords,
    NameRoles,
    Events,
    Roles,
    ResourceLookup,
    ResolverOverview,
    NameHistory,
    ResourceHistory,
    ResourcePermissions,
    NamespaceManifests,
}

pub(crate) const API_ROUTE_DEFINITIONS: &[ApiRouteDefinition] = &[
    ApiRouteDefinition::private_get(ApiRouteId::Health, "/healthz"),
    ApiRouteDefinition::public_get(
        ApiRouteId::PublicStatus,
        "/v1/status",
        ApiRouteContract::new(
            "status",
            "Projection indexing status by chain",
            "Status",
            &[],
            "PublicStatusResponse",
            ApiRouteErrorResponses::new(false, false),
        ),
    ),
    ApiRouteDefinition::public_post(
        ApiRouteId::IdentityLookup,
        "/v1/identity:lookup",
        ApiRouteContract::new(
            "identity_lookup",
            "Native slim identity lookup",
            "Identity",
            &[],
            "IdentityLookupResponse",
            ApiRouteErrorResponses::new(true, false),
        )
        .with_request_schema("IdentityLookupInput"),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Names,
        "/v1/names",
        ApiRouteContract::new(
            "names",
            "App-facing compact name search and exact lookup",
            "Names",
            NAMES_PARAMETERS,
            "CompactNamesResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::AddressNames,
        "/v1/addresses/{address}/names",
        ApiRouteContract::new(
            "address_names",
            "Address-to-surface collection",
            "Collections",
            ADDRESS_NAMES_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::AddressHistory,
        "/v1/history/addresses/{address}",
        ApiRouteContract::new(
            "address_history",
            "Address activity across related surfaces and resources",
            "History",
            ADDRESS_HISTORY_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::PrimaryNames,
        "/v1/primary-names/{address}",
        ApiRouteContract::new(
            "primary_names",
            "Claimed and verified primary-name answer",
            "Resolution",
            PRIMARY_NAMES_PARAMETERS,
            "PrimaryNameResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Coverage,
        "/v1/coverage/{namespace}/{name}",
        ApiRouteContract::new(
            "coverage_current",
            "Single-name coverage and explain details",
            "Coverage",
            EXACT_NAME_SNAPSHOT_PARAMETERS,
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainSurfaceBinding,
        "/v1/explain/names/{namespace}/{name}/surface-binding",
        ApiRouteContract::new(
            "explain_surface_binding_current",
            "Current surface-binding explain view for one exact name",
            "Explain",
            EXACT_NAME_SNAPSHOT_PARAMETERS,
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainAuthorityControl,
        "/v1/explain/names/{namespace}/{name}/authority-control",
        ApiRouteContract::new(
            "explain_authority_control_current",
            "Current authority/control explain view for one exact name",
            "Explain",
            EXACT_NAME_SNAPSHOT_PARAMETERS,
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainResolutionExecution,
        "/v1/explain/resolutions/{namespace}/{name}/execution",
        ApiRouteContract::new(
            "explain_resolution_execution_current",
            "Persisted verified execution explain for one exact-name resolution request",
            "Explain",
            EXPLAIN_RESOLUTION_EXECUTION_PARAMETERS,
            "ResolutionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NamespaceMetadata,
        "/v1/namespaces/{namespace}",
        ApiRouteContract::new(
            "namespace_metadata",
            "Namespace metadata and support status",
            "Namespaces",
            NAMESPACE_PATH_PARAMETERS,
            "NamespaceMetadataResponse",
            ApiRouteErrorResponses::new(false, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameChildren,
        "/v1/names/{namespace}/{name}/children",
        ApiRouteContract::new(
            "name_children",
            "Declared child collection by default",
            "Collections",
            NAME_CHILDREN_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameRecords,
        "/v1/names/{namespace}/{name}/records",
        ApiRouteContract::new(
            "name_records",
            "App-facing compact resolver records",
            "Resolution",
            NAME_RECORDS_PARAMETERS,
            "CompactNameRecordsResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameRoles,
        "/v1/names/{namespace}/{name}/roles",
        ApiRouteContract::new(
            "name_roles",
            "App-facing role rows for a name's current resource",
            "Collections",
            NAME_ROLES_PARAMETERS,
            "CompactRolesResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameCurrent,
        "/v1/names/{namespace}/{name}",
        ApiRouteContract::new(
            "name_current",
            "Exact name lookup",
            "Names",
            EXACT_NAME_SNAPSHOT_PARAMETERS,
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameProfile,
        "/v1/profiles/names/{name}",
        ApiRouteContract::new(
            "name_profile",
            "App-facing inferred-name full profile with declared and verified record results",
            "Names",
            NAME_PROFILE_PARAMETERS,
            "NameProfileResponse",
            ApiRouteErrorResponses::conflict(true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Events,
        "/v1/events",
        ApiRouteContract::new(
            "events",
            "App-facing compact event search",
            "History",
            EVENTS_PARAMETERS,
            "CompactEventsResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Roles,
        "/v1/roles",
        ApiRouteContract::new(
            "roles",
            "App-facing role rows by account, resource, or name filters",
            "Collections",
            ROLES_PARAMETERS,
            "CompactRolesResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourceLookup,
        "/v1/resources/lookup",
        ApiRouteContract::new(
            "resource_lookup",
            "App-facing lookup from name to current resource identity",
            "Resources",
            RESOURCE_LOOKUP_PARAMETERS,
            "ResourceLookupResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolverOverview,
        "/v1/resolvers/{chain_id}/{resolver_address}/overview",
        ApiRouteContract::new(
            "resolver_overview",
            "App-facing compact resolver overview",
            "Resolvers",
            RESOLVER_OVERVIEW_PARAMETERS,
            "CompactResolverOverviewResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameHistory,
        "/v1/history/names/{namespace}/{name}",
        ApiRouteContract::new(
            "name_history",
            "Surface or combined history",
            "History",
            NAME_HISTORY_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourceHistory,
        "/v1/history/resources/{resource_id}",
        ApiRouteContract::new(
            "resource_history",
            "Resource history",
            "History",
            RESOURCE_HISTORY_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourcePermissions,
        "/v1/resources/{resource_id}/permissions",
        ApiRouteContract::new(
            "resource_permissions",
            "Resource-centric effective permissions",
            "Collections",
            RESOURCE_PERMISSIONS_PARAMETERS,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NamespaceManifests,
        "/v1/manifests/{namespace}",
        ApiRouteContract::new(
            "namespace_manifests",
            "Active manifest versions and capabilities",
            "Namespaces",
            NAMESPACE_PATH_PARAMETERS,
            "NamespaceManifestsResponse",
            ApiRouteErrorResponses::new(false, true),
        ),
    ),
];
