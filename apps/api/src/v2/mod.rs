#![allow(dead_code, unused_imports)]

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
mod permissions;
mod primary_name;
mod resolvers;
mod router;
mod snapshots;
mod status;
mod subnames;
mod vocab;

pub(crate) use address_history::get_address_history;
pub(crate) use address_names::{
    AddressName, AddressNameGrant, AddressNamesCursorBinding, address_names_cursor_payload,
    address_names_storage_cursor, build_address_name, build_address_name_role_summary,
    dedupe_to_storage, get_address_names, order_to_storage, permission_scope_value,
    relation_to_storage, sort_to_storage,
};
pub(crate) use chains::{numeric_to_slug, slug_to_numeric};
pub(crate) use cursor::{Payload as CursorPayload, V2_CURSOR_VERSION, decode, encode};
pub(crate) use diag_events::{DiagnosticEvent, build_diagnostic_event, get_diagnostic_events};
pub(crate) use diag_namespace_manifests::get_diagnostic_namespace_manifests;
pub(crate) use diagnostics::{
    get_name_authority_diagnostic, get_name_binding_diagnostic, get_name_coverage_diagnostic,
    get_name_records_diagnostic,
};
pub(crate) use envelope::{AsOf, Envelope, Meta, Page};
pub(crate) use error::{ErrorBody, ErrorCode, ErrorEnvelope, V2Error, V2Result};
pub(crate) use events::{
    Event, build_event, events_cursor_payload, events_storage_cursor, get_events,
};
pub(crate) use history::{
    HistoryEvent, api_error_to_v2, build_history_event, format_timestamp, get_history,
    history_cursor_payload, history_event_type, history_storage_cursor, history_storage_scope,
    v2_exact_name_snapshot_scope,
};
pub(crate) use lookup::get_lookup;
pub(crate) use name_record::{
    NameRecord, build_name_record, classify_registration_status, get_name_record,
};
pub(crate) use name_records::{
    NameRecords, RecordAnswer, VerifiedRecordLookup, build_auto_name_records,
    build_indexed_name_records, build_verified_name_records, get_name_records,
    indexed_records_requiring_verified_fallback, load_ephemeral_verified_record_lookup,
    load_verified_record_lookup,
};
pub(crate) use name_records_inventory::{default_requested_records, validate_product_record};
pub(crate) use namespaces::get_namespace;
pub(crate) use params::{
    AtSelector, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, QueryParams, RawQueryParams, RequestSource,
    SortOrder,
};
pub(crate) use permissions::{PermissionLineage, PermissionRow, get_permissions};
pub(crate) use primary_name::{
    PrimaryName, PrimaryNameAnswer, PrimaryNameQueryParams, PrimaryNameSourceSelection,
    PrimaryNameVerification, build_primary_name, get_primary_name,
};
pub(crate) use resolvers::{
    BoundNames, BoundNamesCursorBinding, ResolverOverview, bound_names_cursor_payload,
    bound_names_storage_cursor, build_bound_name_record, build_resolver_overview, get_resolver,
    resolver_overview_include,
};
pub(crate) use snapshots::{
    as_of_meta, consistency_for_finality, decode_at_token, encode_at_token, resolve_v2_snapshot,
};
pub(crate) use status::get_status;
pub(crate) use subnames::{
    Subname, build_subname, get_subnames, subname_cursor_payload, subname_storage_cursor,
};
pub(crate) use vocab::{
    AddressNamesDedupe, AddressNamesSort, Completeness, Finality, HistoryEventType, HistoryScope,
    OpsStatus, RegistrationStatus, Relation, Resolver, Source, Status,
};

use axum::Router;

use crate::state::AppState;

pub(crate) fn router() -> Router<AppState> {
    router::router()
}
