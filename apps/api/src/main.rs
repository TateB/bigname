#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use bigname_manifests::{NamespaceManifestSnapshot, load_namespace_manifest_snapshot};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe, ChainPositions,
    ChildrenCurrentRow, EventHistoryAddressFilter, EventHistoryFilter, ExecutionCacheKey,
    ExecutionOutcome, ExecutionTrace, HistoryEvent, HistoryScope, HistorySummary,
    HistorySummaryMode, NameCurrentRow, PermissionScope, PermissionsCurrentRow,
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, RecordInventoryCurrentRow, ResolverCurrentRow,
    SelectedSnapshot, SnapshotAt, SnapshotConsistency, SnapshotPositionRequirement,
    SnapshotProjectionRead, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, SurfaceBindingKind,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, load_address_history_page, load_chain_checkpoint,
    load_event_history_page, load_execution_outcome, load_execution_trace, load_name_current,
    load_name_current_for_snapshot, load_name_history_page, load_name_surface,
    load_primary_name_current_snapshot, load_record_inventory_current,
    load_record_inventory_current_for_snapshot, load_resolver_current, load_resource,
    load_resource_history_page, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_resource_id, parse_rfc3339_utc_timestamp,
    resolve_exact_name_snapshot_selection,
};
use clap::Parser;
use serde_json::{Map as JsonMap, json};
use sqlx::{
    PgPool, Row,
    types::{JsonValue, Uuid, time::OffsetDateTime},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

mod cli;
mod errors;
mod graphql;
mod pagination;
mod query;
mod routes;
mod state;
mod types;
mod v2;

use crate::{
    cli::*,
    errors::{ApiError, ApiResult},
    pagination::{
        CURSOR_VERSION, CursorEnvelope, CursorSpec, DEFAULT_PAGE_SIZE, HistoryPageResponse,
        MAX_PAGE_SIZE, PaginationRequest,
    },
    query::{
        AddressHistoryQuery, AddressNamesIncludeOptions, AddressNamesQuery, ChildrenQuery,
        EventsQuery, ExactNameSnapshotQuery, HistoryQuery, MetaMode, NameProfileQuery,
        NameRecordsQuery, NameRolesQuery, NamesQuery, PermissionsQuery, PrimaryNameQuery,
        ResolutionExecutionExplainQuery, ResolutionMode, ResolutionRecordKey,
        ResolverOverviewQuery, ResourceLookupQuery, ResponseView, RolesQuery,
    },
    routes::API_ROUTE_DEFINITIONS,
    state::AppState,
    types::*,
};

#[cfg(test)]
use crate::errors::ErrorResponse;
#[cfg(test)]
use axum::response::Response;

pub(crate) const PUBLIC_NAMESPACES: &[&str] = &["ens", "basenames"];
const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve(args) => {
            init_tracing("bigname-api");
            serve(args).await
        }
        Command::PrintOpenapi => {
            print!("{}", render_openapi_document());
            Ok(())
        }
    }
}

include!("openapi.rs");

include!("handlers.rs");

include!("responses.rs");

include!("support.rs");

#[cfg(test)]
mod tests;
