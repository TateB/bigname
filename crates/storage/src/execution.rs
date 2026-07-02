mod decode;
mod invalidation;
mod keying;
mod outcome;
mod snapshot_match;
mod trace;
mod trace_rows;
mod trace_validation;
mod types;

pub use invalidation::{
    invalidate_execution_outcomes_for_manifest_version,
    invalidate_execution_outcomes_for_manifest_version_and_request_key,
    invalidate_execution_outcomes_for_orphaned_blocks,
    invalidate_execution_outcomes_for_record_boundary,
    invalidate_execution_outcomes_for_record_boundary_and_request_key,
    invalidate_execution_outcomes_for_topology_boundary,
    invalidate_execution_outcomes_for_topology_boundary_and_request_key,
};
pub use outcome::{
    load_execution_outcome, load_resolution_execution_outcome_at_snapshot,
    upsert_execution_outcome, upsert_execution_outcome_in_transaction,
};
pub use trace::{
    load_execution_trace, load_execution_trace_inspection, upsert_execution_trace,
    upsert_execution_trace_in_transaction,
};
pub use types::{
    ExecutionBoundaryInvalidation, ExecutionCacheKey, ExecutionManifestInvalidation,
    ExecutionOutcome, ExecutionOutcomeInvalidationSummary, ExecutionTrace,
    ExecutionTraceInspection, ExecutionTraceStep,
};

#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use sqlx::types::time::OffsetDateTime;
#[cfg(test)]
use uuid::Uuid;

#[cfg(test)]
mod tests;
