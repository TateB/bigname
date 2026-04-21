use std::{collections::BTreeSet, fmt::Write as _};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

/// Persisted execution trace with request, chain, manifest, and ordered-step context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub execution_trace_id: Uuid,
    pub request_type: String,
    pub request_key: String,
    pub namespace: String,
    pub chain_context: Value,
    pub manifest_context: Value,
    pub contracts_called: Value,
    pub gateway_digests: Value,
    pub final_payload: Option<Value>,
    pub failure_payload: Option<Value>,
    pub request_metadata: Value,
    pub finished_at: Option<OffsetDateTime>,
    pub steps: Vec<ExecutionTraceStep>,
}

/// Persisted ordered execution step for one trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTraceStep {
    pub step_index: i64,
    pub step_kind: String,
    pub input_digest: Option<String>,
    pub output_digest: Option<String>,
    pub latency_ms: Option<i64>,
    pub canonicality_dependency: Value,
    pub step_payload: Value,
}

/// Read-only operational inspection snapshot for one persisted execution trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTraceInspection {
    pub trace: ExecutionTrace,
}

/// Deterministic cache identity for one verified execution outcome snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionCacheKey {
    pub request_key: String,
    pub requested_chain_positions: Value,
    pub manifest_versions: Value,
    pub topology_version_boundary: Value,
    pub record_version_boundary: Value,
}

/// Persisted verified execution outcome keyed by the frozen cache boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub cache_key: ExecutionCacheKey,
    pub execution_trace_id: Uuid,
    pub request_type: String,
    pub namespace: String,
    pub outcome_payload: Option<Value>,
    pub failure_payload: Option<Value>,
    pub finished_at: OffsetDateTime,
}

/// Exact stale manifest identity/version that should invalidate persisted execution outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionManifestInvalidation {
    pub request_type: String,
    pub namespace: String,
    pub source_manifest_id: Option<i64>,
    pub source_family: Option<String>,
    pub manifest_version: i64,
}

/// Exact stale topology or record boundary that should invalidate persisted execution outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionBoundaryInvalidation {
    pub request_type: String,
    pub namespace: String,
    pub boundary: Value,
}

/// Summary of one execution-outcome invalidation pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionOutcomeInvalidationSummary {
    pub deleted_outcome_count: u64,
}

/// Load one stored execution trace and its ordered steps.
pub async fn load_execution_trace(
    pool: &PgPool,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTrace>> {
    let Some(mut trace) = load_execution_trace_row_internal(pool, execution_trace_id).await? else {
        return Ok(None);
    };
    trace.steps = load_execution_steps_internal(pool, execution_trace_id).await?;
    Ok(Some(trace))
}

/// Load one persisted execution trace for operational inspection.
///
/// This helper reads only `execution_traces` and `execution_steps`; it does not
/// mutate cache outcomes or start a fresh execution.
pub async fn load_execution_trace_inspection(
    pool: &PgPool,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTraceInspection>> {
    Ok(load_execution_trace(pool, execution_trace_id)
        .await?
        .map(|trace| ExecutionTraceInspection { trace }))
}

/// Insert one execution trace and its ordered steps transactionally, or reload the
/// same trace when the `execution_trace_id` already exists.
pub async fn upsert_execution_trace(
    pool: &PgPool,
    trace: &ExecutionTrace,
) -> Result<ExecutionTrace> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution trace upsert")?;
    let snapshot = upsert_execution_trace_in_transaction(&mut transaction, trace).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution trace upsert")?;

    Ok(snapshot)
}

/// Insert one execution trace and its ordered steps inside an existing
/// transaction so callers can atomically persist related execution writes.
pub async fn upsert_execution_trace_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<ExecutionTrace> {
    validate_execution_trace(trace)?;

    if insert_execution_trace_row(&mut *transaction, trace)
        .await?
        .is_some()
    {
        insert_execution_steps(&mut *transaction, trace).await?;
    }

    let mut snapshot =
        load_execution_trace_row_internal(&mut **transaction, trace.execution_trace_id)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload execution trace {} after upsert",
                    trace.execution_trace_id
                )
            })?;
    snapshot.steps =
        load_execution_steps_internal(&mut **transaction, trace.execution_trace_id).await?;
    ensure_execution_trace_identity_matches(&snapshot, trace)?;

    Ok(snapshot)
}

/// Load one cached verified execution outcome by the frozen execution cache key.
pub async fn load_execution_outcome(
    pool: &PgPool,
    cache_key: &ExecutionCacheKey,
) -> Result<Option<ExecutionOutcome>> {
    let execution_cache_key = execution_cache_key_storage_key(cache_key)
        .context("failed to derive execution cache key")?;
    load_execution_outcome_row_internal(pool, &execution_cache_key).await
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

/// Delete cached execution outcomes for one exact stale manifest identity/version.
pub async fn invalidate_execution_outcomes_for_manifest_version(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_manifest_version_internal(pool, invalidation, None).await
}

/// Delete cached execution outcomes for one exact stale manifest identity/version and
/// one exact request key.
pub async fn invalidate_execution_outcomes_for_manifest_version_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution manifest invalidation",
    )?;
    invalidate_execution_outcomes_for_manifest_version_internal(
        pool,
        invalidation,
        Some(request_key.as_str()),
    )
    .await
}

async fn invalidate_execution_outcomes_for_manifest_version_internal(
    pool: &PgPool,
    invalidation: &ExecutionManifestInvalidation,
    request_key: Option<&str>,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let invalidation = normalize_execution_manifest_invalidation(invalidation)?;
    let target_identity = invalidation.identity_key();

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution manifest invalidation")?;

    let outcomes = load_execution_outcomes_for_scope_internal(
        &mut *transaction,
        &invalidation.request_type,
        &invalidation.namespace,
    )
    .await?;
    let mut cache_keys = Vec::new();
    for outcome in outcomes {
        if !outcome_matches_request_key(&outcome, request_key) {
            continue;
        }
        let manifest_versions = decode_manifest_versions(
            &outcome.cache_key.manifest_versions,
            &outcome.cache_key.request_key,
        )?;
        if manifest_versions
            .iter()
            .any(|manifest_version| manifest_version.identity_key() == target_identity)
        {
            cache_keys.push(execution_cache_key_storage_key(&outcome.cache_key)?);
        }
    }

    let deleted_outcome_count =
        delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution manifest invalidation")?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

/// Delete cached execution outcomes for one exact stale topology boundary.
pub async fn invalidate_execution_outcomes_for_topology_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        None,
        "topology_version_boundary",
        |outcome| &outcome.cache_key.topology_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale topology boundary and
/// one exact request key.
pub async fn invalidate_execution_outcomes_for_topology_boundary_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution topology boundary invalidation",
    )?;
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        Some(request_key.as_str()),
        "topology_version_boundary",
        |outcome| &outcome.cache_key.topology_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale record boundary.
pub async fn invalidate_execution_outcomes_for_record_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        None,
        "record_version_boundary",
        |outcome| &outcome.cache_key.record_version_boundary,
    )
    .await
}

/// Delete cached execution outcomes for one exact stale record boundary and one exact
/// request key.
pub async fn invalidate_execution_outcomes_for_record_boundary_and_request_key(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let request_key = normalize_execution_invalidation_request_key(
        request_key,
        "execution record boundary invalidation",
    )?;
    invalidate_execution_outcomes_for_boundary(
        pool,
        invalidation,
        Some(request_key.as_str()),
        "record_version_boundary",
        |outcome| &outcome.cache_key.record_version_boundary,
    )
    .await
}

/// Delete verified resolution and verified primary-name cache outcomes whose
/// cache dependencies reference a block identity marked `orphaned`.
pub async fn invalidate_execution_outcomes_for_orphaned_blocks(
    pool: &PgPool,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution reorg invalidation")?;

    let orphaned_blocks = load_orphaned_block_dependencies_internal(&mut *transaction).await?;
    let outcomes =
        load_execution_outcomes_for_reorg_invalidation_scope_internal(&mut *transaction).await?;

    let mut cache_keys = Vec::new();
    for outcome in outcomes {
        let dependencies = execution_outcome_block_dependencies(&outcome).with_context(|| {
            format!(
                "execution outcome for request_type {} namespace {} request_key {} cannot be associated with explicit block-hash-bearing dependencies",
                outcome.request_type, outcome.namespace, outcome.cache_key.request_key
            )
        })?;
        if dependencies
            .iter()
            .any(|dependency| orphaned_blocks.contains(dependency))
        {
            cache_keys.push(execution_cache_key_storage_key(&outcome.cache_key)?);
        }
    }

    let deleted_outcome_count =
        delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution reorg invalidation")?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

async fn insert_execution_trace_row(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<Option<ExecutionTrace>> {
    let chain_context = serde_json::to_string(&trace.chain_context)
        .context("failed to serialize execution trace chain_context")?;
    let manifest_context = serde_json::to_string(&trace.manifest_context)
        .context("failed to serialize execution trace manifest_context")?;
    let contracts_called = serde_json::to_string(&trace.contracts_called)
        .context("failed to serialize execution trace contracts_called")?;
    let gateway_digests = serde_json::to_string(&trace.gateway_digests)
        .context("failed to serialize execution trace gateway_digests")?;
    let final_payload = trace
        .final_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution trace final_payload")?;
    let failure_payload = trace
        .failure_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution trace failure_payload")?;
    let request_metadata = serde_json::to_string(&trace.request_metadata)
        .context("failed to serialize execution trace request_metadata")?;

    let row = sqlx::query(
        r#"
        INSERT INTO execution_traces (
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5::jsonb,
            $6::jsonb,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11::jsonb,
            $12
        )
        ON CONFLICT (execution_trace_id) DO NOTHING
        RETURNING
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        "#,
    )
    .bind(trace.execution_trace_id)
    .bind(&trace.request_type)
    .bind(&trace.request_key)
    .bind(&trace.namespace)
    .bind(chain_context)
    .bind(manifest_context)
    .bind(contracts_called)
    .bind(gateway_digests)
    .bind(final_payload)
    .bind(failure_payload)
    .bind(request_metadata)
    .bind(trace.finished_at)
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert execution trace {}",
            trace.execution_trace_id
        )
    })?;

    row.map(decode_execution_trace_row).transpose()
}

async fn insert_execution_steps(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<()> {
    for step in &trace.steps {
        let canonicality_dependency = serde_json::to_string(&step.canonicality_dependency)
            .context("failed to serialize execution step canonicality_dependency")?;
        let step_payload = serde_json::to_string(&step.step_payload)
            .context("failed to serialize execution step payload")?;

        sqlx::query(
            r#"
            INSERT INTO execution_steps (
                execution_trace_id,
                step_index,
                step_kind,
                input_digest,
                output_digest,
                latency_ms,
                canonicality_dependency,
                step_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
            "#,
        )
        .bind(trace.execution_trace_id)
        .bind(step.step_index)
        .bind(&step.step_kind)
        .bind(&step.input_digest)
        .bind(&step.output_digest)
        .bind(step.latency_ms)
        .bind(canonicality_dependency)
        .bind(step_payload)
        .execute(&mut **executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert execution step {} for trace {}",
                step.step_index, trace.execution_trace_id
            )
        })?;
    }

    Ok(())
}

async fn load_execution_trace_row_internal<'e, E>(
    executor: E,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTrace>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        FROM execution_traces
        WHERE execution_trace_id = $1
        "#,
    )
    .bind(execution_trace_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load execution trace {execution_trace_id}"))?;

    row.map(decode_execution_trace_row).transpose()
}

async fn load_execution_steps_internal<'e, E>(
    executor: E,
    execution_trace_id: Uuid,
) -> Result<Vec<ExecutionTraceStep>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            step_index,
            step_kind,
            input_digest,
            output_digest,
            latency_ms,
            canonicality_dependency,
            step_payload
        FROM execution_steps
        WHERE execution_trace_id = $1
        ORDER BY step_index
        "#,
    )
    .bind(execution_trace_id)
    .fetch_all(executor)
    .await
    .with_context(|| format!("failed to load execution steps for trace {execution_trace_id}"))?;

    rows.into_iter().map(decode_execution_step).collect()
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

async fn load_execution_outcomes_for_scope_internal<'e, E>(
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

async fn load_execution_outcomes_for_reorg_invalidation_scope_internal<'e, E>(
    executor: E,
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
        WHERE request_type IN ('verified_resolution', 'verified_primary_name')
        ORDER BY execution_cache_key
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load execution outcomes for reorg invalidation scope")?;

    rows.into_iter().map(decode_execution_outcome_row).collect()
}

async fn load_orphaned_block_dependencies_internal<'e, E>(
    executor: E,
) -> Result<BTreeSet<(String, String)>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT chain_id, block_hash
        FROM chain_lineage
        WHERE canonicality_state = 'orphaned'::canonicality_state
        ORDER BY chain_id, block_hash
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load orphaned block identities for execution reorg invalidation")?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("chain_id")
                    .context("orphaned lineage row missing chain_id")?,
                row.try_get("block_hash")
                    .context("orphaned lineage row missing block_hash")?,
            ))
        })
        .collect()
}

async fn delete_execution_outcomes_by_keys(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    execution_cache_keys: &[String],
) -> Result<u64> {
    let mut deleted_outcome_count = 0;
    for execution_cache_key in execution_cache_keys {
        deleted_outcome_count += sqlx::query(
            r#"
            DELETE FROM execution_cache_outcomes
            WHERE execution_cache_key = $1
            "#,
        )
        .bind(execution_cache_key)
        .execute(&mut **executor)
        .await
        .with_context(|| {
            format!("failed to delete execution outcome for cache key {execution_cache_key}")
        })?
        .rows_affected();
    }

    Ok(deleted_outcome_count)
}

fn validate_execution_trace(trace: &ExecutionTrace) -> Result<()> {
    if trace.request_type.is_empty() {
        bail!(
            "execution trace {} has empty request_type",
            trace.execution_trace_id
        );
    }
    if trace.request_key.is_empty() {
        bail!(
            "execution trace {} has empty request_key",
            trace.execution_trace_id
        );
    }
    if trace.namespace.is_empty() {
        bail!(
            "execution trace {} has empty namespace",
            trace.execution_trace_id
        );
    }
    ensure_nonempty_json_object(
        &trace.chain_context,
        "chain_context",
        trace.execution_trace_id,
    )?;
    ensure_nonempty_json_object(
        &trace.manifest_context,
        "manifest_context",
        trace.execution_trace_id,
    )?;
    ensure_json_array(
        &trace.contracts_called,
        "contracts_called",
        trace.execution_trace_id,
    )?;
    ensure_json_array(
        &trace.gateway_digests,
        "gateway_digests",
        trace.execution_trace_id,
    )?;
    ensure_json_object(
        &trace.request_metadata,
        "request_metadata",
        trace.execution_trace_id,
    )?;
    if trace.finished_at.is_none() {
        bail!(
            "execution trace {} must set finished_at",
            trace.execution_trace_id
        );
    }
    if trace.final_payload.is_none() && trace.failure_payload.is_none() {
        bail!(
            "execution trace {} must set final_payload or failure_payload",
            trace.execution_trace_id
        );
    }
    if trace.steps.is_empty() {
        bail!(
            "execution trace {} must include at least one step",
            trace.execution_trace_id
        );
    }

    for (expected_index, step) in trace.steps.iter().enumerate() {
        let expected_index = i64::try_from(expected_index)
            .context("execution trace step index does not fit in i64")?;
        if step.step_index != expected_index {
            bail!(
                "execution trace {} step order must be contiguous from 0; expected index {}, found {}",
                trace.execution_trace_id,
                expected_index,
                step.step_index
            );
        }
        validate_execution_step(trace.execution_trace_id, step)?;
    }

    Ok(())
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

impl ExecutionManifestInvalidation {
    fn identity_key(&self) -> String {
        ManifestVersionParts {
            source_manifest_id: self.source_manifest_id,
            source_family: self.source_family.clone(),
            manifest_version: self.manifest_version,
        }
        .identity_key()
    }
}

fn normalize_execution_manifest_invalidation(
    invalidation: &ExecutionManifestInvalidation,
) -> Result<ExecutionManifestInvalidation> {
    let request_type = invalidation.request_type.trim();
    if request_type.is_empty() {
        bail!("execution manifest invalidation has empty request_type");
    }

    let namespace = invalidation.namespace.trim();
    if namespace.is_empty() {
        bail!("execution manifest invalidation has empty namespace");
    }

    let source_manifest_id = match invalidation.source_manifest_id {
        Some(value) if value > 0 => Some(value),
        Some(value) => bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} source_manifest_id must be positive, got {value}"
        ),
        None => None,
    };
    let source_family = match invalidation.source_family.as_deref() {
        Some(value) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Some(_) => bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} source_family must be non-empty when present"
        ),
        None => None,
    };
    if source_manifest_id.is_none() && source_family.is_none() {
        bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} must include source_manifest_id or source_family"
        );
    }
    if invalidation.manifest_version <= 0 {
        bail!(
            "execution manifest invalidation for request_type {request_type} namespace {namespace} manifest_version must be positive, got {}",
            invalidation.manifest_version
        );
    }

    Ok(ExecutionManifestInvalidation {
        request_type: request_type.to_owned(),
        namespace: namespace.to_owned(),
        source_manifest_id,
        source_family,
        manifest_version: invalidation.manifest_version,
    })
}

fn normalize_execution_boundary_invalidation(
    invalidation: &ExecutionBoundaryInvalidation,
    field_name: &str,
) -> Result<ExecutionBoundaryInvalidation> {
    let request_type = invalidation.request_type.trim();
    if request_type.is_empty() {
        bail!("execution boundary invalidation has empty request_type");
    }

    let namespace = invalidation.namespace.trim();
    if namespace.is_empty() {
        bail!("execution boundary invalidation has empty namespace");
    }

    validate_version_boundary(
        &invalidation.boundary,
        field_name,
        &format!("{request_type}/{namespace}"),
    )?;

    Ok(ExecutionBoundaryInvalidation {
        request_type: request_type.to_owned(),
        namespace: namespace.to_owned(),
        boundary: invalidation.boundary.clone(),
    })
}

async fn invalidate_execution_outcomes_for_boundary(
    pool: &PgPool,
    invalidation: &ExecutionBoundaryInvalidation,
    request_key: Option<&str>,
    field_name: &str,
    boundary: impl Fn(&ExecutionOutcome) -> &Value,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    let invalidation = normalize_execution_boundary_invalidation(invalidation, field_name)?;
    let target_boundary = version_boundary_storage_key(
        &invalidation.boundary,
        field_name,
        &format!("{}/{}", invalidation.request_type, invalidation.namespace),
    )?;

    let mut transaction = pool.begin().await.with_context(|| {
        format!("failed to open transaction for execution {field_name} invalidation")
    })?;

    let outcomes = load_execution_outcomes_for_scope_internal(
        &mut *transaction,
        &invalidation.request_type,
        &invalidation.namespace,
    )
    .await?;
    let mut cache_keys = Vec::new();
    for outcome in outcomes {
        if !outcome_matches_request_key(&outcome, request_key) {
            continue;
        }
        let outcome_boundary = version_boundary_storage_key(
            boundary(&outcome),
            field_name,
            &outcome.cache_key.request_key,
        )?;
        if outcome_boundary == target_boundary {
            cache_keys.push(execution_cache_key_storage_key(&outcome.cache_key)?);
        }
    }

    let deleted_outcome_count =
        delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;

    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit execution {field_name} invalidation"))?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

fn normalize_execution_cache_key(cache_key: &ExecutionCacheKey) -> Result<ExecutionCacheKey> {
    let request_key = cache_key.request_key.trim();
    if request_key.is_empty() {
        bail!("execution cache key has empty request_key");
    }

    let requested_chain_positions =
        normalize_requested_chain_positions(&cache_key.requested_chain_positions, request_key)?;
    let manifest_versions = normalize_manifest_versions(&cache_key.manifest_versions, request_key)?;
    validate_version_boundary(
        &cache_key.topology_version_boundary,
        "topology_version_boundary",
        request_key,
    )?;
    validate_version_boundary(
        &cache_key.record_version_boundary,
        "record_version_boundary",
        request_key,
    )?;

    Ok(ExecutionCacheKey {
        request_key: request_key.to_owned(),
        requested_chain_positions,
        manifest_versions,
        topology_version_boundary: cache_key.topology_version_boundary.clone(),
        record_version_boundary: cache_key.record_version_boundary.clone(),
    })
}

fn normalize_execution_invalidation_request_key(
    request_key: &str,
    context: &str,
) -> Result<String> {
    let request_key = request_key.trim();
    if request_key.is_empty() {
        bail!("{context} has empty request_key");
    }
    Ok(request_key.to_owned())
}

fn outcome_matches_request_key(outcome: &ExecutionOutcome, request_key: Option<&str>) -> bool {
    request_key.is_none_or(|request_key| outcome.cache_key.request_key == request_key)
}

fn execution_cache_key_storage_key(cache_key: &ExecutionCacheKey) -> Result<String> {
    let normalized = normalize_execution_cache_key(cache_key)?;
    let requested_positions = decode_requested_chain_positions(
        &normalized.requested_chain_positions,
        &normalized.request_key,
    )?;
    let manifest_versions =
        decode_manifest_versions(&normalized.manifest_versions, &normalized.request_key)?;
    let topology_version_boundary = decode_version_boundary(
        &normalized.topology_version_boundary,
        "topology_version_boundary",
        &normalized.request_key,
    )?;
    let record_version_boundary = decode_version_boundary(
        &normalized.record_version_boundary,
        "record_version_boundary",
        &normalized.request_key,
    )?;

    let mut key = String::new();
    append_key_part(&mut key, &normalized.request_key);

    for position in requested_positions {
        append_key_part(&mut key, &position.chain_id);
        append_key_part(&mut key, &position.block_number.to_string());
        append_key_part(&mut key, &position.block_hash);
    }

    for manifest_version in manifest_versions {
        append_key_part(
            &mut key,
            &manifest_version
                .source_manifest_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        append_key_part(
            &mut key,
            manifest_version
                .source_family
                .as_deref()
                .unwrap_or_default(),
        );
        append_key_part(&mut key, &manifest_version.manifest_version.to_string());
    }

    append_version_boundary_key_parts(&mut key, &topology_version_boundary);
    append_version_boundary_key_parts(&mut key, &record_version_boundary);

    Ok(key)
}

fn validate_execution_step(execution_trace_id: Uuid, step: &ExecutionTraceStep) -> Result<()> {
    if step.step_kind.is_empty() {
        bail!(
            "execution trace {} step {} has empty step_kind",
            execution_trace_id,
            step.step_index
        );
    }
    if step.step_index < 0 {
        bail!(
            "execution trace {} step {} has negative step_index",
            execution_trace_id,
            step.step_index
        );
    }
    if let Some(latency_ms) = step.latency_ms
        && latency_ms < 0
    {
        bail!(
            "execution trace {} step {} has negative latency_ms {}",
            execution_trace_id,
            step.step_index,
            latency_ms
        );
    }
    ensure_nonempty_json_object(
        &step.canonicality_dependency,
        "canonicality_dependency",
        execution_trace_id,
    )?;
    ensure_json_object(&step.step_payload, "step_payload", execution_trace_id)?;

    Ok(())
}

fn normalize_requested_chain_positions(value: &Value, request_key: &str) -> Result<Value> {
    let positions = decode_requested_chain_positions(value, request_key)?;
    let normalized = positions
        .iter()
        .map(RequestedChainPositionParts::to_value)
        .collect::<Vec<_>>();
    Ok(Value::Array(normalized))
}

fn decode_requested_chain_positions(
    value: &Value,
    request_key: &str,
) -> Result<Vec<RequestedChainPositionParts>> {
    let items = value.as_array().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} requested_chain_positions must be a JSON array"
        )
    })?;
    if items.is_empty() {
        bail!(
            "execution outcome cache key for request_key {request_key} requested_chain_positions must not be empty"
        );
    }

    let mut positions = Vec::with_capacity(items.len());
    let mut seen_chain_ids = BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} requested_chain_positions[{index}] must be a JSON object"
            )
        })?;
        let position = RequestedChainPositionParts::from_object(object, request_key, index)?;
        if !seen_chain_ids.insert(position.chain_id.clone()) {
            bail!(
                "execution outcome cache key for request_key {request_key} requested_chain_positions must not repeat chain_id {}",
                position.chain_id
            );
        }
        positions.push(position);
    }

    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

fn normalize_manifest_versions(value: &Value, request_key: &str) -> Result<Value> {
    let manifest_versions = decode_manifest_versions(value, request_key)?;
    let normalized = manifest_versions
        .iter()
        .map(ManifestVersionParts::to_value)
        .collect::<Vec<_>>();
    Ok(Value::Array(normalized))
}

fn decode_manifest_versions(value: &Value, request_key: &str) -> Result<Vec<ManifestVersionParts>> {
    let items = value.as_array().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} manifest_versions must be a JSON array"
        )
    })?;
    if items.is_empty() {
        bail!(
            "execution outcome cache key for request_key {request_key} manifest_versions must not be empty"
        );
    }

    let mut manifest_versions = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} manifest_versions[{index}] must be a JSON object"
            )
        })?;
        let manifest_version = ManifestVersionParts::from_object(object, request_key, index)?;
        if !seen.insert(manifest_version.identity_key()) {
            bail!(
                "execution outcome cache key for request_key {request_key} manifest_versions must not repeat the same manifest identity"
            );
        }
        manifest_versions.push(manifest_version);
    }

    manifest_versions.sort_by(|left, right| left.identity_key().cmp(&right.identity_key()));
    Ok(manifest_versions)
}

fn validate_version_boundary(value: &Value, field_name: &str, request_key: &str) -> Result<()> {
    decode_version_boundary(value, field_name, request_key).map(|_| ())
}

fn version_boundary_storage_key(
    value: &Value,
    field_name: &str,
    request_key: &str,
) -> Result<String> {
    let boundary = decode_version_boundary(value, field_name, request_key)?;
    let mut key = String::new();
    append_version_boundary_key_parts(&mut key, &boundary);
    Ok(key)
}

fn execution_outcome_block_dependencies(
    outcome: &ExecutionOutcome,
) -> Result<BTreeSet<(String, String)>> {
    let mut dependencies = BTreeSet::new();
    for position in decode_requested_chain_positions(
        &outcome.cache_key.requested_chain_positions,
        &outcome.cache_key.request_key,
    )? {
        dependencies.insert((position.chain_id, position.block_hash));
    }

    let topology_boundary = decode_version_boundary(
        &outcome.cache_key.topology_version_boundary,
        "topology_version_boundary",
        &outcome.cache_key.request_key,
    )?;
    dependencies.insert((
        topology_boundary.chain_position.chain_id,
        topology_boundary.chain_position.block_hash,
    ));

    let record_boundary = decode_version_boundary(
        &outcome.cache_key.record_version_boundary,
        "record_version_boundary",
        &outcome.cache_key.request_key,
    )?;
    dependencies.insert((
        record_boundary.chain_position.chain_id,
        record_boundary.chain_position.block_hash,
    ));

    if dependencies.is_empty() {
        bail!(
            "execution outcome for request_key {} has no block-hash-bearing dependencies",
            outcome.cache_key.request_key
        );
    }

    Ok(dependencies)
}

fn decode_version_boundary(
    value: &Value,
    field_name: &str,
    request_key: &str,
) -> Result<VersionBoundaryParts> {
    let object = value.as_object().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} {field_name} must be a JSON object"
        )
    })?;
    let logical_name_id =
        required_string_field(object, "logical_name_id", field_name, request_key)?.to_owned();
    let resource_id = Uuid::parse_str(required_string_field(
        object,
        "resource_id",
        field_name,
        request_key,
    )?)
    .with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} {field_name}.resource_id must be a UUID"
        )
    })?;
    let normalized_event_id = match object.get("normalized_event_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {field_name}.normalized_event_id must be null or positive integer"
            )
        })?),
        None => bail!(
            "execution outcome cache key for request_key {request_key} {field_name} must include normalized_event_id"
        ),
    };
    let event_kind = match object.get("event_kind") {
        Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => bail!(
            "execution outcome cache key for request_key {request_key} {field_name}.event_kind must be null or non-empty string"
        ),
        None => bail!(
            "execution outcome cache key for request_key {request_key} {field_name} must include event_kind"
        ),
    };
    if normalized_event_id.is_some() != event_kind.is_some() {
        bail!(
            "execution outcome cache key for request_key {request_key} {field_name} normalized_event_id and event_kind must both be present or both be null"
        );
    }
    let chain_position = decode_chain_position(
        object.get("chain_position").with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {field_name} must include chain_position"
            )
        })?,
        &format!("{field_name}.chain_position"),
        request_key,
    )?;

    Ok(VersionBoundaryParts {
        logical_name_id,
        resource_id,
        normalized_event_id,
        event_kind,
        chain_position,
    })
}

fn append_version_boundary_key_parts(buffer: &mut String, boundary: &VersionBoundaryParts) {
    append_key_part(buffer, &boundary.logical_name_id);
    append_key_part(buffer, &boundary.resource_id.to_string());
    append_key_part(
        buffer,
        &boundary
            .normalized_event_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(buffer, boundary.event_kind.as_deref().unwrap_or_default());
    append_key_part(buffer, &boundary.chain_position.chain_id);
    append_key_part(buffer, &boundary.chain_position.block_number.to_string());
    append_key_part(buffer, &boundary.chain_position.block_hash);
    append_key_part(buffer, &boundary.chain_position.timestamp);
}

fn validate_optional_nonnull_json_value(
    value: &Option<Value>,
    field_name: &str,
    request_key: &str,
) -> Result<()> {
    if value.as_ref().is_some_and(Value::is_null) {
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

fn ensure_json_object(value: &Value, field_name: &str, execution_trace_id: Uuid) -> Result<()> {
    if !value.is_object() {
        bail!(
            "execution trace {} field {} must be a JSON object",
            execution_trace_id,
            field_name
        );
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

fn append_key_part(buffer: &mut String, value: &str) {
    write!(buffer, "{}:{value};", value.len()).expect("string write to key buffer must succeed");
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
    request_key: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {context} must include non-empty string field {field_name}"
            )
        })
}

fn optional_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
    request_key: &str,
) -> Result<Option<&'a str>> {
    match object.get(field_name) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        Some(_) => bail!(
            "execution outcome cache key for request_key {request_key} {context}.{field_name} must be null or non-empty string"
        ),
    }
}

fn decode_chain_position(
    value: &Value,
    context: &str,
    request_key: &str,
) -> Result<ChainPositionParts> {
    let object = value.as_object().with_context(|| {
        format!("execution outcome cache key for request_key {request_key} {context} must be a JSON object")
    })?;
    let chain_id = required_string_field(object, "chain_id", context, request_key)?.to_owned();
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {context} must include non-negative integer block_number"
            )
        })?;
    let block_hash = required_string_field(object, "block_hash", context, request_key)?.to_owned();
    let timestamp = required_string_field(object, "timestamp", context, request_key)?.to_owned();

    Ok(ChainPositionParts {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

#[derive(Clone, Debug)]
struct RequestedChainPositionParts {
    chain_id: String,
    block_number: i64,
    block_hash: String,
}

impl RequestedChainPositionParts {
    fn from_object(object: &Map<String, Value>, request_key: &str, index: usize) -> Result<Self> {
        let context = format!("requested_chain_positions[{index}]");
        let chain_id = required_string_field(object, "chain_id", &context, request_key)?.to_owned();
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
            .with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context} must include non-negative integer block_number"
                )
            })?;
        let block_hash =
            required_string_field(object, "block_hash", &context, request_key)?.to_owned();

        Ok(Self {
            chain_id,
            block_number,
            block_hash,
        })
    }

    fn to_value(&self) -> Value {
        serde_json::json!({
            "chain_id": self.chain_id,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
        })
    }
}

#[derive(Clone, Debug)]
struct ManifestVersionParts {
    source_manifest_id: Option<i64>,
    source_family: Option<String>,
    manifest_version: i64,
}

impl ManifestVersionParts {
    fn from_object(object: &Map<String, Value>, request_key: &str, index: usize) -> Result<Self> {
        let context = format!("manifest_versions[{index}]");
        let source_manifest_id = match object.get("source_manifest_id") {
            Some(Value::Null) | None => None,
            Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context}.source_manifest_id must be null or positive integer"
                )
            })?),
        };
        let source_family = optional_string_field(object, "source_family", &context, request_key)?
            .map(str::to_owned);
        if source_manifest_id.is_none() && source_family.is_none() {
            bail!(
                "execution outcome cache key for request_key {request_key} {context} must include source_manifest_id or source_family"
            );
        }
        let manifest_version = object
            .get("manifest_version")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context} must include positive integer manifest_version"
                )
            })?;

        Ok(Self {
            source_manifest_id,
            source_family,
            manifest_version,
        })
    }

    fn identity_key(&self) -> String {
        let mut key = String::new();
        append_key_part(
            &mut key,
            &self
                .source_manifest_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        append_key_part(&mut key, self.source_family.as_deref().unwrap_or_default());
        append_key_part(&mut key, &self.manifest_version.to_string());
        key
    }

    fn to_value(&self) -> Value {
        let mut object = Map::new();
        if let Some(source_manifest_id) = self.source_manifest_id {
            object.insert(
                "source_manifest_id".to_owned(),
                Value::Number(source_manifest_id.into()),
            );
        }
        if let Some(source_family) = &self.source_family {
            object.insert(
                "source_family".to_owned(),
                Value::String(source_family.clone()),
            );
        }
        object.insert(
            "manifest_version".to_owned(),
            Value::Number(self.manifest_version.into()),
        );
        Value::Object(object)
    }
}

#[derive(Clone, Debug)]
struct VersionBoundaryParts {
    logical_name_id: String,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<String>,
    chain_position: ChainPositionParts,
}

#[derive(Clone, Debug)]
struct ChainPositionParts {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}

fn ensure_nonempty_json_object(
    value: &Value,
    field_name: &str,
    execution_trace_id: Uuid,
) -> Result<()> {
    ensure_json_object(value, field_name, execution_trace_id)?;

    if value.as_object().is_some_and(|object| object.is_empty()) {
        bail!(
            "execution trace {} field {} must not be empty",
            execution_trace_id,
            field_name
        );
    }

    Ok(())
}

fn ensure_json_array(value: &Value, field_name: &str, execution_trace_id: Uuid) -> Result<()> {
    if !value.is_array() {
        bail!(
            "execution trace {} field {} must be a JSON array",
            execution_trace_id,
            field_name
        );
    }

    Ok(())
}

fn ensure_execution_trace_identity_matches(
    existing: &ExecutionTrace,
    incoming: &ExecutionTrace,
) -> Result<()> {
    if existing.request_type != incoming.request_type
        || existing.request_key != incoming.request_key
        || existing.namespace != incoming.namespace
        || existing.chain_context != incoming.chain_context
        || existing.manifest_context != incoming.manifest_context
        || existing.contracts_called != incoming.contracts_called
        || existing.gateway_digests != incoming.gateway_digests
        || existing.final_payload != incoming.final_payload
        || existing.failure_payload != incoming.failure_payload
        || existing.request_metadata != incoming.request_metadata
        || existing.finished_at != incoming.finished_at
        || existing.steps != incoming.steps
    {
        bail!(
            "execution trace identity mismatch for trace {}",
            existing.execution_trace_id
        );
    }

    Ok(())
}

fn decode_execution_trace_row(row: PgRow) -> Result<ExecutionTrace> {
    Ok(ExecutionTrace {
        execution_trace_id: row
            .try_get("execution_trace_id")
            .context("missing execution_trace_id")?,
        request_type: row
            .try_get("request_type")
            .context("missing request_type")?,
        request_key: row.try_get("request_key").context("missing request_key")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        chain_context: row
            .try_get("chain_context")
            .context("missing chain_context")?,
        manifest_context: row
            .try_get("manifest_context")
            .context("missing manifest_context")?,
        contracts_called: row
            .try_get("contracts_called")
            .context("missing contracts_called")?,
        gateway_digests: row
            .try_get("gateway_digests")
            .context("missing gateway_digests")?,
        final_payload: row
            .try_get("final_payload")
            .context("missing final_payload")?,
        failure_payload: row
            .try_get("failure_payload")
            .context("missing failure_payload")?,
        request_metadata: row
            .try_get("request_metadata")
            .context("missing request_metadata")?,
        finished_at: row.try_get("finished_at").context("missing finished_at")?,
        steps: Vec::new(),
    })
}

fn decode_execution_step(row: PgRow) -> Result<ExecutionTraceStep> {
    Ok(ExecutionTraceStep {
        step_index: row.try_get("step_index").context("missing step_index")?,
        step_kind: row.try_get("step_kind").context("missing step_kind")?,
        input_digest: row
            .try_get("input_digest")
            .context("missing input_digest")?,
        output_digest: row
            .try_get("output_digest")
            .context("missing output_digest")?,
        latency_ms: row.try_get("latency_ms").context("missing latency_ms")?,
        canonicality_dependency: row
            .try_get("canonicality_dependency")
            .context("missing canonicality_dependency")?,
        step_payload: row
            .try_get("step_payload")
            .context("missing step_payload")?,
    })
}

#[cfg(test)]
mod tests;
