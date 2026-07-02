use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::snapshot_selection::ChainPositions;

use super::{
    keying::{execution_cache_key_storage_key, normalize_execution_cache_key},
    snapshot_match::resolution_execution_outcome_is_at_or_before_snapshot,
    types::{ExecutionCacheKey, ExecutionOutcome},
};

const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

/// Load one cached verified execution outcome by the frozen execution cache key.
pub async fn load_execution_outcome(
    pool: &PgPool,
    cache_key: &ExecutionCacheKey,
) -> Result<Option<ExecutionOutcome>> {
    let execution_cache_key = execution_cache_key_storage_key(cache_key)
        .context("failed to derive execution cache key")?;
    load_execution_outcome_row_internal(pool, &execution_cache_key).await
}

/// Load the newest persisted resolution execution outcome for the selected
/// non-position cache identity whose requested chain positions are at or
/// before the selected snapshot.
pub async fn load_resolution_execution_outcome_at_snapshot(
    pool: &PgPool,
    cache_key: &ExecutionCacheKey,
    snapshot_positions: &ChainPositions,
) -> Result<Option<ExecutionOutcome>> {
    let normalized_cache_key = normalize_execution_cache_key(cache_key)
        .context("failed to normalize execution cache key")?;
    let manifest_versions = serde_json::to_string(&normalized_cache_key.manifest_versions)
        .context("failed to serialize selected manifest_versions")?;
    let topology_version_boundary =
        serde_json::to_string(&normalized_cache_key.topology_version_boundary)
            .context("failed to serialize selected topology_version_boundary")?;
    let record_version_boundary =
        serde_json::to_string(&normalized_cache_key.record_version_boundary)
            .context("failed to serialize selected record_version_boundary")?;
    let request_key_prefix = execution_cache_request_key_prefix(&normalized_cache_key.request_key);
    let request_key_prefix_upper_bound =
        execution_cache_request_key_prefix_upper_bound(&request_key_prefix);

    let rows = sqlx::query(
        r#"
        SELECT
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND request_key = $2
          AND manifest_versions = $3::jsonb
          AND topology_version_boundary = $4::jsonb
          AND record_version_boundary = $5::jsonb
          AND execution_cache_key >= $6
          AND execution_cache_key < $7
        ORDER BY finished_at DESC NULLS LAST, execution_trace_id DESC
        "#,
    )
    .bind(VERIFIED_RESOLUTION_REQUEST_TYPE)
    .bind(&normalized_cache_key.request_key)
    .bind(manifest_versions)
    .bind(topology_version_boundary)
    .bind(record_version_boundary)
    .bind(request_key_prefix)
    .bind(request_key_prefix_upper_bound)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolution execution outcome candidates for request_key {}",
            normalized_cache_key.request_key
        )
    })?;

    for row in rows {
        let outcome = decode_execution_outcome_row(row)?;
        if resolution_execution_outcome_is_at_or_before_snapshot(pool, &outcome, snapshot_positions)
            .await?
        {
            return Ok(Some(outcome));
        }
    }

    Ok(None)
}

fn execution_cache_request_key_prefix(request_key: &str) -> String {
    let mut prefix = String::new();
    super::decode::append_key_part(&mut prefix, request_key);
    prefix
}

fn execution_cache_request_key_prefix_upper_bound(prefix: &str) -> String {
    debug_assert!(prefix.ends_with(';'));
    let mut upper_bound = prefix.to_owned();
    upper_bound.pop();
    upper_bound.push('<');
    upper_bound
}

/// Insert or replace one verified execution outcome keyed by the frozen execution cache key.
pub async fn upsert_execution_outcome(
    pool: &PgPool,
    outcome: &ExecutionOutcome,
) -> Result<ExecutionOutcome> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution outcome upsert")?;
    let snapshot = upsert_execution_outcome_in_transaction(&mut transaction, outcome).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution outcome upsert")?;

    Ok(snapshot)
}

/// Insert or replace one verified execution outcome inside an existing
/// transaction so callers can atomically persist related execution writes.
pub async fn upsert_execution_outcome_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    outcome: &ExecutionOutcome,
) -> Result<ExecutionOutcome> {
    let normalized = normalize_execution_outcome(outcome)?;
    let execution_cache_key = execution_cache_key_storage_key(&normalized.cache_key)
        .context("failed to derive execution cache key for execution outcome upsert")?;

    if let Some(existing) =
        load_execution_outcome_row_internal(&mut **transaction, &execution_cache_key).await?
    {
        ensure_execution_outcome_identity_matches(&existing, &normalized, &execution_cache_key)?;
    }

    upsert_execution_outcome_row(&mut *transaction, &normalized, &execution_cache_key).await?;

    let snapshot = load_execution_outcome_row_internal(&mut **transaction, &execution_cache_key)
        .await?
        .with_context(|| {
            format!(
                "failed to reload execution outcome for cache key {execution_cache_key} after upsert"
            )
        })?;

    Ok(snapshot)
}

pub(super) async fn load_execution_outcomes_for_scope_internal<'e, E>(
    executor: E,
    request_type: &str,
    namespace: &str,
) -> Result<Vec<ExecutionOutcome>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
        ORDER BY execution_cache_key
        "#,
    )
    .bind(request_type)
    .bind(namespace)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load execution outcomes for request_type {request_type} and namespace {namespace}"
        )
    })?;

    rows.into_iter().map(decode_execution_outcome_row).collect()
}

async fn upsert_execution_outcome_row(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    outcome: &ExecutionOutcome,
    execution_cache_key: &str,
) -> Result<()> {
    let requested_chain_positions =
        serde_json::to_string(&outcome.cache_key.requested_chain_positions)
            .context("failed to serialize execution outcome requested_chain_positions")?;
    let manifest_versions = serde_json::to_string(&outcome.cache_key.manifest_versions)
        .context("failed to serialize execution outcome manifest_versions")?;
    let topology_version_boundary =
        serde_json::to_string(&outcome.cache_key.topology_version_boundary)
            .context("failed to serialize execution outcome topology_version_boundary")?;
    let record_version_boundary = serde_json::to_string(&outcome.cache_key.record_version_boundary)
        .context("failed to serialize execution outcome record_version_boundary")?;
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution outcome payload")?;
    let failure_payload = outcome
        .failure_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution outcome failure_payload")?;

    sqlx::query(
        r#"
        INSERT INTO execution_cache_outcomes (
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11::jsonb,
            $12
        )
        ON CONFLICT (execution_cache_key) DO UPDATE
        SET
            request_key = EXCLUDED.request_key,
            requested_chain_positions = EXCLUDED.requested_chain_positions,
            manifest_versions = EXCLUDED.manifest_versions,
            topology_version_boundary = EXCLUDED.topology_version_boundary,
            record_version_boundary = EXCLUDED.record_version_boundary,
            execution_trace_id = EXCLUDED.execution_trace_id,
            request_type = EXCLUDED.request_type,
            namespace = EXCLUDED.namespace,
            outcome_payload = EXCLUDED.outcome_payload,
            failure_payload = EXCLUDED.failure_payload,
            finished_at = EXCLUDED.finished_at,
            updated_at = now()
        "#,
    )
    .bind(execution_cache_key)
    .bind(&outcome.cache_key.request_key)
    .bind(requested_chain_positions)
    .bind(manifest_versions)
    .bind(topology_version_boundary)
    .bind(record_version_boundary)
    .bind(outcome.execution_trace_id)
    .bind(&outcome.request_type)
    .bind(&outcome.namespace)
    .bind(outcome_payload)
    .bind(failure_payload)
    .bind(outcome.finished_at)
    .execute(&mut **executor)
    .await
    .with_context(|| {
        format!("failed to upsert execution outcome for cache key {execution_cache_key}")
    })?;

    Ok(())
}

async fn load_execution_outcome_row_internal<'e, E>(
    executor: E,
    execution_cache_key: &str,
) -> Result<Option<ExecutionOutcome>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE execution_cache_key = $1
        "#,
    )
    .bind(execution_cache_key)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load execution outcome for cache key {execution_cache_key}")
    })?;

    row.map(decode_execution_outcome_row).transpose()
}

fn normalize_execution_outcome(outcome: &ExecutionOutcome) -> Result<ExecutionOutcome> {
    let normalized_cache_key = normalize_execution_cache_key(&outcome.cache_key)
        .context("execution outcome has invalid cache key")?;

    if outcome.request_type.trim().is_empty() {
        bail!(
            "execution outcome for request_key {} has empty request_type",
            normalized_cache_key.request_key
        );
    }
    if outcome.namespace.trim().is_empty() {
        bail!(
            "execution outcome for request_key {} has empty namespace",
            normalized_cache_key.request_key
        );
    }
    validate_optional_nonnull_json_value(
        &outcome.outcome_payload,
        "outcome_payload",
        &normalized_cache_key.request_key,
    )?;
    validate_optional_nonnull_json_value(
        &outcome.failure_payload,
        "failure_payload",
        &normalized_cache_key.request_key,
    )?;
    if outcome.outcome_payload.is_none() && outcome.failure_payload.is_none() {
        bail!(
            "execution outcome for request_key {} must set outcome_payload or failure_payload",
            normalized_cache_key.request_key
        );
    }

    Ok(ExecutionOutcome {
        cache_key: normalized_cache_key,
        execution_trace_id: outcome.execution_trace_id,
        request_type: outcome.request_type.trim().to_owned(),
        namespace: outcome.namespace.trim().to_owned(),
        outcome_payload: outcome.outcome_payload.clone(),
        failure_payload: outcome.failure_payload.clone(),
        finished_at: outcome.finished_at,
    })
}

fn validate_optional_nonnull_json_value(
    value: &Option<serde_json::Value>,
    field_name: &str,
    request_key: &str,
) -> Result<()> {
    if value.as_ref().is_some_and(serde_json::Value::is_null) {
        bail!("execution outcome for request_key {request_key} {field_name} must not be JSON null");
    }
    Ok(())
}

fn ensure_execution_outcome_identity_matches(
    existing: &ExecutionOutcome,
    incoming: &ExecutionOutcome,
    execution_cache_key: &str,
) -> Result<()> {
    if existing.request_type != incoming.request_type || existing.namespace != incoming.namespace {
        bail!("execution outcome cache identity mismatch for cache key {execution_cache_key}");
    }

    Ok(())
}

fn decode_execution_outcome_row(row: PgRow) -> Result<ExecutionOutcome> {
    let cache_key = ExecutionCacheKey {
        request_key: row
            .try_get("request_key")
            .context("execution outcome row missing request_key")?,
        requested_chain_positions: row
            .try_get("requested_chain_positions")
            .context("execution outcome row missing requested_chain_positions")?,
        manifest_versions: row
            .try_get("manifest_versions")
            .context("execution outcome row missing manifest_versions")?,
        topology_version_boundary: row
            .try_get("topology_version_boundary")
            .context("execution outcome row missing topology_version_boundary")?,
        record_version_boundary: row
            .try_get("record_version_boundary")
            .context("execution outcome row missing record_version_boundary")?,
    };
    let decoded_execution_cache_key = execution_cache_key_storage_key(&cache_key)?;
    let stored_execution_cache_key: String = row
        .try_get("execution_cache_key")
        .context("execution outcome row missing execution_cache_key")?;
    if stored_execution_cache_key != decoded_execution_cache_key {
        bail!(
            "execution outcome cache key mismatch: stored {}, decoded {}",
            stored_execution_cache_key,
            decoded_execution_cache_key
        );
    }

    normalize_execution_outcome(&ExecutionOutcome {
        cache_key,
        execution_trace_id: row
            .try_get("execution_trace_id")
            .context("execution outcome row missing execution_trace_id")?,
        request_type: row
            .try_get("request_type")
            .context("execution outcome row missing request_type")?,
        namespace: row
            .try_get("namespace")
            .context("execution outcome row missing namespace")?,
        outcome_payload: row
            .try_get("outcome_payload")
            .context("execution outcome row missing outcome_payload")?,
        failure_payload: row
            .try_get("failure_payload")
            .context("execution outcome row missing failure_payload")?,
        finished_at: row
            .try_get("finished_at")
            .context("execution outcome row missing finished_at")?,
    })
}
