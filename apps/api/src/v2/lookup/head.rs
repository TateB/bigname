use bigname_storage::{
    SnapshotConsistency, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, resolve_exact_name_snapshot_selection,
};
use sqlx::PgPool;

use crate::v2::{
    Meta, SnapshotReadResource, V2Error, V2Result, sanitized_snapshot_internal_error, snapshot_meta,
};

pub(crate) async fn load_served_head_meta(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> V2Result<Meta> {
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(|_| V2Error::internal_error("failed to build lookup served head selector"))?;
    let selected = match resolve_exact_name_snapshot_selection(pool, scope, &input).await {
        Ok(selected) => selected,
        Err(error) if served_head_absent_for_single_scope(scope, &error) => {
            return Ok(Meta::default());
        }
        Err(error) if served_head_scope_conflict(&error) => {
            return Err(V2Error::conflict(
                "served head is unavailable for snapshot scope",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::InvalidInput => {
            return Err(V2Error::internal_error(
                "failed to build lookup served head selector",
            ));
        }
        Err(error) => {
            return Err(sanitized_snapshot_internal_error(
                &error,
                SnapshotReadResource::Resource,
            ));
        }
    };
    snapshot_meta(&selected)
}

fn served_head_absent_for_single_scope(
    scope: &SnapshotSelectionScope,
    error: &SnapshotSelectionError,
) -> bool {
    scope.required_positions().len() == 1
        && error.kind() == SnapshotSelectionErrorKind::Conflict
        && (error.message().contains("has no stored checkpoint row")
            || error.message().contains("has no head checkpoint"))
}

fn served_head_scope_conflict(error: &SnapshotSelectionError) -> bool {
    error.kind() == SnapshotSelectionErrorKind::Conflict
        || error.message().contains("mismatched hash and number")
}
