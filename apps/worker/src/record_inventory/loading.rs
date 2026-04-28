use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{constants::*, json::parse_canonicality_state, types::RelevantEvent};

pub(super) async fn load_all_relevant_events(pool: &PgPool) -> Result<Vec<RelevantEvent>> {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
    let rows = sqlx::query(&format!(
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
        LEFT JOIN raw_blocks rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        LEFT JOIN raw_logs rl
          ON rl.chain_id = ne.chain_id
         AND rl.block_hash = ne.block_hash
         AND rl.log_index = ne.log_index
        WHERE ne.derivation_kind = ANY($1::TEXT[])
          AND ne.event_kind IN ($2, $3, $4)
          AND (ne.event_kind <> $4 OR ne.namespace = ANY($5::TEXT[]))
          AND ne.resource_id IS NOT NULL
          AND ne.logical_name_id IS NOT NULL
          AND ne.chain_id IS NOT NULL
          AND ne.block_number IS NOT NULL
          AND ne.block_hash IS NOT NULL
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.resource_id ASC,
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
    .fetch_all(pool)
    .await
    .context("failed to load record_inventory_current events")?;

    rows.into_iter().map(decode_relevant_event).collect()
}

pub(super) async fn load_relevant_events(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<RelevantEvent>> {
    let derivation_kinds = record_inventory_derivation_kinds();
    let resolver_event_namespaces = resolver_event_namespaces();
    let rows = sqlx::query(&format!(
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
        LEFT JOIN raw_blocks rb
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

    rows.into_iter().map(decode_relevant_event).collect()
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

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: row.try_get("normalized_event_id")?,
        logical_name_id: row
            .try_get::<Option<String>, _>("logical_name_id")?
            .context("record event must include logical_name_id")?,
        resource_id: row
            .try_get::<Option<Uuid>, _>("resource_id")?
            .context("record event must include resource_id")?,
        event_kind: row.try_get("event_kind")?,
        source_family: row.try_get("source_family")?,
        manifest_version: row.try_get("manifest_version")?,
        source_manifest_id: row.try_get("source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")?
            .context("record event must include chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")?
            .context("record event must include block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")?
            .context("record event must include block_hash")?,
        block_timestamp: row.try_get("block_timestamp")?,
        raw_fact_ref: row.try_get("raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")?,
        )?,
        after_state: row.try_get("after_state")?,
        emitting_address: row.try_get("emitting_address")?,
    })
}
