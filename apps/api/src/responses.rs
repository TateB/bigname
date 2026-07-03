include!("responses/namespaces.rs");

include!("responses/resolution.rs");

include!("responses/resolution_verified.rs");

include!("responses/collections.rs");

include!("responses/projections.rs");

use crate::v2::format_timestamp;

pub(crate) mod responses {
    pub(crate) use super::{
        build_name_authority_control_explain_declared_state, build_name_coverage_declared_state,
        build_name_surface_binding_explain_declared_state, build_record_cache_section_for_name,
        build_record_inventory_section_for_name,
    };
}

include!("responses/history.rs");

include!("responses/app_facing/names_collection.rs");

include!("responses/app_facing/identity.rs");

include!("responses/app_facing/identity_native.rs");

include!("responses/app_facing/records.rs");

include!("responses/app_facing/events.rs");

include!("responses/app_facing/roles.rs");

include!("responses/app_facing/resolver_overview.rs");

include!("responses/json.rs");
