mod address_history;
mod address_names;
mod chains;
mod cursor;
mod diag_events;
mod diag_namespace_manifests;
mod diagnostics;
mod envelope;
mod error;
mod events;
mod history;
mod lookup;
mod name_record;
mod name_records;
mod name_records_inventory;
mod namespaces;
mod params;
mod permission_values;
mod permissions;
mod primary_name;
mod resolvers;
mod router;
mod search;
mod snapshots;
mod status;
mod strict_query;
mod subnames;
mod vocab;

pub(crate) use address_history::get_address_history;
pub(crate) use address_names::{AddressNameGrant, get_address_names};
pub(crate) use chains::{numeric_to_slug, slug_to_numeric, snapshot_slot_for_slug};
pub(crate) use cursor::{Payload as CursorPayload, decode, encode};
pub(crate) use diag_events::get_diagnostic_events;
pub(crate) use diag_namespace_manifests::get_diagnostic_namespace_manifests;
pub(crate) use diagnostics::{
    get_name_authority_diagnostic, get_name_binding_diagnostic, get_name_coverage_diagnostic,
    get_name_execution_diagnostic, get_name_records_diagnostic,
};
pub(crate) use envelope::{Envelope, Meta, Page};
#[cfg(test)]
pub(crate) use error::ErrorCode;
pub(crate) use error::{V2Error, V2Result};
pub(crate) use events::{
    Event, build_event, events_cursor_payload, events_storage_cursor, get_events,
};
pub(crate) use history::{
    format_timestamp, get_history, history_event_type, history_storage_scope,
    v2_exact_name_snapshot_scope, v2_exact_name_snapshot_scope_with_resolution_auxiliary,
};
pub(crate) use lookup::{get_lookup, load_served_head_meta};
pub(crate) use name_record::{NameRecord, build_name_record, get_name_record};
pub(crate) use name_records::{
    RecordAnswer, build_indexed_name_records, build_verified_name_records, get_name_records,
    load_ephemeral_verified_record_lookup, load_persisted_verified_record_lookup,
    parse_record_keys,
};
pub(crate) use name_records_inventory::{default_requested_records, validate_product_record};
pub(crate) use namespaces::get_namespace;
pub(crate) use params::{
    AtSelector, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, QueryParams, RawQueryParams, RequestSource,
    SortOrder, parse_relation_set_param,
};
pub(crate) use permission_values::{permission_powers_value, permission_scope_value};
pub(crate) use permissions::get_permissions;
pub(crate) use primary_name::get_primary_name;
pub(crate) use resolvers::get_resolver;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use resolvers::{
    BoundNames, BoundNamesCursorBinding, bound_names_cursor_payload, bound_names_storage_cursor,
    build_resolver_overview, resolver_overview_include,
};
pub(crate) use search::get_search;
pub(crate) use snapshots::{
    SnapshotReadResource, api_error_to_v2, api_error_to_v2_for_resource, decode_at_token,
    encode_at_token, resolve_v2_snapshot_for, sanitized_snapshot_internal_error, snapshot_meta,
};
pub(crate) use status::get_status;
pub(crate) use strict_query::{
    NoQueryParams, QueryParamAllowlist, StrictQueryParams, parse_raw_query_params_with_allowlist,
};
pub(crate) use subnames::get_subnames;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use vocab::matched_boundary_vocabulary_terms;
pub(crate) use vocab::{
    AddressNamesDedupe, AddressNamesSort, Completeness, Finality, HistoryEventType, HistoryScope,
    OpsStatus, PRODUCT_PIPELINE_TERMS, RegistrationStatus, Relation, RelationSet, Resolver, Source,
    Status, contains_boundary_vocabulary, shared_product_reason,
};

use axum::Router;

use crate::state::AppState;

pub(crate) fn router() -> Router<AppState> {
    router::router()
}
