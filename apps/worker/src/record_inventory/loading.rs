use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{constants::*, types::RelevantEvent};

pub(super) fn stream_target_resource_ids<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<Uuid>> + 'a {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
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
        WHERE ne.derivation_kind = ANY($1::TEXT[])
          AND ne.event_kind IN ($2, $3, $4)
          AND (ne.event_kind <> $4 OR ne.namespace = ANY($5::TEXT[]))
          AND ne.resource_id IS NOT NULL
          AND ne.logical_name_id IS NOT NULL
          AND ne.chain_id IS NOT NULL
          AND ne.block_number IS NOT NULL
          AND ne.block_hash IS NOT NULL
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY ne.resource_id
        "#,
    )
    .bind(derivation_kinds)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(resolver_event_namespaces)
    .fetch(pool)
    .map(|row| {
        row.context("failed to stream resource_ids for record_inventory_current rebuild")
            .and_then(|row| row.try_get("resource_id").context("missing resource_id"))
    })
}

pub(super) async fn load_relevant_events(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<RelevantEvent>> {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
    let rows = sqlx::query_as::<_, RelevantEvent>(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.logical_name_id,
            ne.resource_id,
            ne.event_kind,
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
            ne.after_state,
            LOWER(rl.emitting_address) AS emitting_address
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
        LEFT JOIN raw_logs rl
          ON rl.chain_id = ne.chain_id
         AND rl.block_hash = ne.block_hash
         AND rl.log_index = ne.log_index
        WHERE ne.derivation_kind = ANY($1::TEXT[])
          AND ne.event_kind IN ($2, $3, $4)
          AND (ne.event_kind <> $4 OR ne.namespace = ANY($5::TEXT[]))
          AND ne.resource_id = $6
          AND ne.logical_name_id IS NOT NULL
          AND ne.chain_id IS NOT NULL
          AND ne.block_number IS NOT NULL
          AND ne.block_hash IS NOT NULL
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number ASC,
            ne.log_index ASC NULLS FIRST,
            ne.normalized_event_id ASC
        "#
    ))
    .bind(&derivation_kinds)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(&resolver_event_namespaces)
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current events for resource_id {resource_id}")
    })?;

    Ok(rows)
}

fn record_inventory_derivation_kinds() -> Vec<String> {
    vec![
        DERIVATION_KIND_DECLARED_AUTHORITY.to_owned(),
        DERIVATION_KIND_ENS_V2_RESOLVER.to_owned(),
    ]
}

fn resolver_event_namespaces() -> Vec<String> {
    vec![ENS_NAMESPACE.to_owned(), BASENAMES_NAMESPACE.to_owned()]
}
