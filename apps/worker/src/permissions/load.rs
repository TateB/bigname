use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::types::RelevantEvent;
use super::{CANONICAL_STATE_FILTER, EVENT_KIND_PERMISSION_CHANGED};

pub(super) fn stream_target_resource_ids<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<Uuid>> + 'a {
    sqlx::query(
        r#"
        SELECT DISTINCT ne.resource_id
        FROM normalized_events ne
        JOIN resources resource
          ON resource.resource_id = ne.resource_id
         AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        WHERE ne.event_kind = $1
          AND ne.resource_id IS NOT NULL
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY ne.resource_id
        "#,
    )
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .fetch(pool)
    .map(|row| {
        row.context("failed to stream resource_ids for permissions_current rebuild")
            .and_then(|row| row.try_get("resource_id").context("missing resource_id"))
    })
}

pub(super) async fn load_permission_events(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<RelevantEvent>> {
    let rows = sqlx::query_as::<_, RelevantEvent>(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.resource_id,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            ne.log_index,
            rb.block_timestamp,
            ne.raw_fact_ref,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.after_state
        FROM normalized_events ne
        JOIN resources resource
          ON resource.resource_id = ne.resource_id
         AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        WHERE ne.event_kind = $1
          AND ne.resource_id = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(EVENT_KIND_PERMISSION_CHANGED)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical PermissionChanged events for resource_id {resource_id}")
    })?;

    Ok(rows)
}
