use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;

use crate::routes::{
    ApiParameterLocation, ApiParameterSchema, ApiRouteContract, ApiRouteDefinition, ApiRouteId,
    ApiRouteMethod, ApiRouteParameter,
};
use crate::{
    AppState, Router, address_history, address_names, address_names_count, coverage_current, events,
    explain_authority_control_current, explain_resolution_execution_current,
    explain_surface_binding_current, get, health, name_children, name_current, name_history,
    name_records, name_roles, names, namespace_manifests, namespace_metadata, primary_names,
    resolution_current, resolve_current, resolve_records, resolver_current, resolver_overview,
    resource_history, resource_lookup, resource_permissions, roles,
};

use super::responses::{OpenApiOperationExt, openapi_json_get_operation};

impl ApiRouteDefinition {
    pub(super) fn register(self, router: Router<AppState>) -> Router<AppState> {
        match self.method {
            ApiRouteMethod::Get => self.register_get(router),
        }
    }

    fn register_get(self, router: Router<AppState>) -> Router<AppState> {
        match self.id {
            ApiRouteId::Health => router.route(self.path, get(health)),
            ApiRouteId::Names => router.route(self.path, get(names)),
            ApiRouteId::AddressNames => router.route(self.path, get(address_names)),
            ApiRouteId::AddressNamesCount => router.route(self.path, get(address_names_count)),
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
            ApiRouteId::NameRecords => router.route(self.path, get(name_records)),
            ApiRouteId::NameRoles => router.route(self.path, get(name_roles)),
            ApiRouteId::Events => router.route(self.path, get(events)),
            ApiRouteId::Roles => router.route(self.path, get(roles)),
            ApiRouteId::ResourceLookup => router.route(self.path, get(resource_lookup)),
            ApiRouteId::ResolveCurrent => router.route(self.path, get(resolve_current)),
            ApiRouteId::ResolveRecords => router.route(self.path, get(resolve_records)),
            ApiRouteId::ResolutionCurrent => router.route(self.path, get(resolution_current)),
            ApiRouteId::ResolverCurrent => router.route(self.path, get(resolver_current)),
            ApiRouteId::ResolverOverview => router.route(self.path, get(resolver_overview)),
            ApiRouteId::NameHistory => router.route(self.path, get(name_history)),
            ApiRouteId::ResourceHistory => router.route(self.path, get(resource_history)),
            ApiRouteId::ResourcePermissions => router.route(self.path, get(resource_permissions)),
            ApiRouteId::NamespaceManifests => router.route(self.path, get(namespace_manifests)),
        }
    }

    pub(super) fn openapi_path_item(self) -> Option<JsonValue> {
        let contract = self.contract?;
        let mut path_item = JsonMap::new();
        path_item.insert(
            self.method.openapi_key().to_owned(),
            contract.openapi_operation(),
        );
        Some(JsonValue::Object(path_item))
    }
}

impl ApiRouteMethod {
    fn openapi_key(self) -> &'static str {
        match self {
            Self::Get => "get",
        }
    }
}

impl ApiRouteContract {
    fn openapi_operation(self) -> JsonValue {
        let errors = self.errors;
        let mut operation = openapi_json_get_operation(
            self.operation_id,
            self.summary,
            self.tag,
            self.parameters
                .iter()
                .copied()
                .map(ApiRouteParameter::openapi_parameter)
                .collect(),
            self.success_schema,
            errors.include_bad_request,
            errors.include_not_found,
        );
        if let Some(description) = errors.bad_request_description {
            operation = operation.with_bad_request_description(description);
        }
        if errors.include_conflict {
            operation = operation.with_conflict_response();
        }
        operation
    }
}

impl ApiRouteParameter {
    fn openapi_parameter(self) -> JsonValue {
        let mut parameter = JsonMap::new();
        parameter.insert("name".to_owned(), JsonValue::String(self.name.to_owned()));
        parameter.insert(
            "in".to_owned(),
            JsonValue::String(self.location.openapi_value().to_owned()),
        );
        parameter.insert("required".to_owned(), JsonValue::Bool(self.required));
        parameter.insert(
            "description".to_owned(),
            JsonValue::String(self.description.to_owned()),
        );
        parameter.insert("schema".to_owned(), self.schema.openapi_schema());
        if self.csv {
            parameter.insert("style".to_owned(), JsonValue::String("form".to_owned()));
            parameter.insert("explode".to_owned(), JsonValue::Bool(false));
        }
        JsonValue::Object(parameter)
    }
}

impl ApiParameterLocation {
    fn openapi_value(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::Query => "query",
        }
    }
}

impl ApiParameterSchema {
    fn openapi_schema(self) -> JsonValue {
        match self {
            Self::String => json!({ "type": "string" }),
            Self::Boolean => json!({ "type": "boolean" }),
            Self::UuidString => json!({ "type": "string", "format": "uuid" }),
            Self::StringPattern(pattern) => json!({ "type": "string", "pattern": pattern }),
            Self::StringEnum(values) => json!({ "type": "string", "enum": values }),
            Self::StringDefault(default) => json!({ "type": "string", "default": default }),
            Self::StringEnumDefault { values, default } => {
                json!({ "type": "string", "enum": values, "default": default })
            }
            Self::IntegerMin(minimum) => json!({ "type": "integer", "minimum": minimum }),
            Self::IntegerRange { minimum, maximum } => {
                json!({ "type": "integer", "minimum": minimum, "maximum": maximum })
            }
        }
    }
}
