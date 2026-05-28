#[path = "handlers/collections.rs"]
mod handler_collections;
#[path = "handlers/app_facing/events.rs"]
mod handler_app_facing_events;
#[path = "handlers/app_facing/identity/mod.rs"]
mod handler_app_facing_identity;
#[path = "handlers/app_facing/names_collection.rs"]
mod handler_app_facing_names_collection;
#[path = "handlers/app_facing/records.rs"]
mod handler_app_facing_records;
#[path = "handlers/app_facing/resolver_overview.rs"]
mod handler_app_facing_resolver_overview;
#[path = "handlers/app_facing/roles.rs"]
mod handler_app_facing_roles;
#[path = "handlers/exact_name.rs"]
mod handler_exact_name;
#[path = "handlers/health.rs"]
mod handler_health;
#[path = "handlers/history.rs"]
mod handler_history;
#[path = "handlers/namespaces.rs"]
mod handler_namespaces;
#[path = "handlers/primary_names.rs"]
mod handler_primary_names;
#[path = "handlers/resolution.rs"]
mod handler_resolution;
#[path = "handlers/resolution_on_demand.rs"]
mod handler_resolution_on_demand;

use self::{
    handler_app_facing_events::events,
    handler_app_facing_identity::{identity_lookup, public_status},
    handler_app_facing_names_collection::names,
    handler_app_facing_records::{name_records, warm_compact_records_route_sql_path},
    handler_app_facing_resolver_overview::resolver_overview,
    handler_app_facing_roles::{name_roles, resource_lookup, roles},
    handler_collections::{address_names, name_children, resource_permissions},
    handler_exact_name::{
        coverage_current, explain_authority_control_current, explain_surface_binding_current,
        name_current,
    },
    handler_health::health,
    handler_history::{address_history, name_history, resource_history},
    handler_namespaces::{namespace_manifests, namespace_metadata},
    handler_primary_names::primary_names,
    handler_resolution::{explain_resolution_execution_current, name_profile},
};
