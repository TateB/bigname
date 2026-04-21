//! Shared PostgreSQL bootstrap utilities.

mod address_names;
mod audit;
mod backfill_jobs;
mod checkpoints;
mod children;
mod execution;
mod history;
mod identity;
mod lineage;
mod name_current;
mod normalized_events;
mod permissions;
mod primary_name;
mod raw;
mod raw_calls;
mod raw_children;
mod raw_code;
mod record_inventory;
mod resolver;

use anyhow::{Context, Result};
use clap::Args;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::info;

pub use address_names::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation, AddressNamesCurrentDedupe,
    clear_address_names_current, collapse_address_name_current_rows, delete_address_names_current,
    load_address_names_current, load_address_names_current_including_noncanonical,
    upsert_address_names_current_rows,
};
pub use audit::{
    CanonicalityInspection, CanonicalityInspectionStatus, RawFactAuditCounts,
    StoredLineageRangeBlock, inspect_block_canonicality, inspect_canonicality_range,
    list_stored_lineage_range,
};
pub use backfill_jobs::{
    BackfillJob, BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec, advance_backfill_range, complete_backfill_job, complete_backfill_range,
    create_backfill_job, fail_backfill_job, fail_backfill_range, load_backfill_job,
    load_backfill_ranges, reserve_backfill_range,
};
pub use checkpoints::{
    ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, advance_chain_checkpoints,
    sync_chain_checkpoints,
};
pub use children::{
    ChildrenCurrentRow, DeclaredChildEventSource, clear_children_current, delete_children_current,
    load_canonical_declared_child_sources, load_canonical_ens_v1_declared_child_sources,
    load_children_current, load_children_current_including_noncanonical,
    upsert_children_current_rows,
};
pub use execution::{
    ExecutionBoundaryInvalidation, ExecutionCacheKey, ExecutionManifestInvalidation,
    ExecutionOutcome, ExecutionOutcomeInvalidationSummary, ExecutionTrace, ExecutionTraceStep,
    invalidate_execution_outcomes_for_manifest_version,
    invalidate_execution_outcomes_for_manifest_version_and_request_key,
    invalidate_execution_outcomes_for_orphaned_blocks,
    invalidate_execution_outcomes_for_record_boundary,
    invalidate_execution_outcomes_for_record_boundary_and_request_key,
    invalidate_execution_outcomes_for_topology_boundary,
    invalidate_execution_outcomes_for_topology_boundary_and_request_key, load_execution_outcome,
    load_execution_trace, upsert_execution_outcome, upsert_execution_outcome_in_transaction,
    upsert_execution_trace, upsert_execution_trace_in_transaction,
};
pub use history::{
    HistoryEvent, HistoryScope, load_address_history, load_name_history, load_name_history_head,
    load_resource_history,
};
pub use identity::{
    IdentityOrphanCounts, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
    load_name_surface, load_name_surface_including_noncanonical, load_resource,
    load_resource_including_noncanonical, load_surface_binding,
    load_surface_binding_including_noncanonical, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_logical_name_id_including_noncanonical,
    load_surface_bindings_by_resource_id,
    load_surface_bindings_by_resource_id_including_noncanonical, load_token_lineage,
    load_token_lineage_including_noncanonical, mark_identity_rows_range_orphaned,
    mark_surface_binding_range_orphaned, upsert_name_surfaces, upsert_resources,
    upsert_surface_bindings, upsert_token_lineages,
};
pub use lineage::{
    CanonicalityState, ChainLineageBlock, load_chain_lineage_block,
    mark_chain_lineage_range_orphaned, upsert_chain_lineage_blocks,
};
pub use name_current::{
    NameCurrentRow, clear_name_current, delete_name_current, load_name_current,
    upsert_name_current_rows,
};
pub use normalized_events::{
    NormalizedEvent, load_normalized_event_counts_by_kind, load_normalized_events_by_namespace,
    mark_block_derived_normalized_events_range_orphaned, upsert_normalized_events,
};
pub use permissions::{
    PermissionScope, PermissionsCurrentRow, clear_permissions_current, delete_permissions_current,
    load_permissions_current, load_permissions_current_for_resolver_scope,
    load_permissions_current_resolver_targets, upsert_permissions_current_rows,
};
pub use primary_name::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, VerifiedPrimaryNameClaimHooks,
    VerifiedPrimaryNameInvalidationHook, VerifiedPrimaryNameLookupHook,
    clear_primary_names_current, delete_primary_name_current, load_primary_name_current,
    load_primary_name_current_snapshot, upsert_primary_name_current_rows,
    upsert_primary_name_current_snapshots, verified_primary_name_claim_hooks,
};
pub use raw::{
    RawBlock, RawLogReplayInput, list_canonical_raw_log_replay_inputs,
    list_canonical_raw_log_replay_inputs_for_block_hashes, load_raw_block,
    load_raw_blocks_by_hashes, mark_raw_block_range_orphaned, upsert_raw_blocks,
};
pub use raw_calls::{
    RawCallSnapshot, load_raw_call_snapshots_by_block_hash, upsert_raw_call_snapshots,
    upsert_raw_call_snapshots_in_transaction,
};
pub use raw_children::{
    RawFactOrphanCounts, RawLog, RawReceipt, RawTransaction, mark_raw_block_facts_range_orphaned,
    upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};
pub use raw_code::{
    RawCodeHash, load_raw_code_hash_counts_by_block_hashes, upsert_raw_code_hashes,
};
pub use record_inventory::{
    RecordInventoryCurrentRow, clear_record_inventory_current, delete_record_inventory_current,
    load_record_inventory_current, upsert_record_inventory_current_rows,
};
pub use resolver::{
    ResolverCurrentRow, clear_resolver_current, delete_resolver_current, load_resolver_current,
    upsert_resolver_current_rows,
};

/// Checked-in migrations for the bootstrap workspace.
pub const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Common database settings shared by the bootstrap binaries.
#[derive(Args, Clone, Debug)]
pub struct DatabaseConfig {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    pub database_url: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_DATABASE_MAX_CONNECTIONS",
        default_value_t = 10_u32
    )]
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_url: Some(default_database_url().to_owned()),
            max_connections: 10,
        }
    }
}

/// Default bootstrap database URL for local development.
pub const fn default_database_url() -> &'static str {
    "postgres://bigname:bigname@127.0.0.1:5432/bigname"
}

/// Open a PostgreSQL connection pool using the shared bootstrap settings.
pub async fn connect(config: &DatabaseConfig) -> Result<PgPool> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| default_database_url().to_owned());

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")
}

/// Apply all checked-in migrations.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR
        .run(pool)
        .await
        .context("failed to apply checked-in migrations")?;
    info!("checked-in migrations applied");
    Ok(())
}
