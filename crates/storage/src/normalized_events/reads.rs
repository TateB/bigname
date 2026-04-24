use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres, Row};

use super::{decode::decode_normalized_event, types::NormalizedEvent};

/// Load normalized events for one namespace in insertion order.
pub async fn load_normalized_events_by_namespace(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<NormalizedEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE namespace = $1
        ORDER BY normalized_event_id
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load normalized events for namespace {namespace}"))?;

    rows.into_iter().map(decode_normalized_event).collect()
}

/// Count normalized events by kind for one namespace.
pub async fn load_normalized_event_counts_by_kind(
    pool: &PgPool,
    namespace: &str,
) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        WHERE namespace = $1
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load normalized-event counts for namespace {namespace}"))?;

    rows.into_iter()
        .map(|row| {
            let event_kind = row
                .try_get::<String, _>("event_kind")
                .context("missing event_kind from normalized-event count row")?;
            let event_count = row
                .try_get::<i64, _>("event_count")
                .context("missing event_count from normalized-event count row")?;
            Ok((
                event_kind,
                usize::try_from(event_count)
                    .context("normalized-event count does not fit in usize")?,
            ))
        })
        .collect()
}

pub(super) async fn load_normalized_event_by_identity<'e, E>(
    executor: E,
    event_identity: &str,
) -> Result<Option<NormalizedEvent>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE event_identity = $1
        "#,
    )
    .bind(event_identity)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load normalized event {event_identity}"))?;

    row.map(decode_normalized_event).transpose()
}
