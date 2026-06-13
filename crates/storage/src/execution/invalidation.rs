use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use super::{
    keying::{
        execution_cache_key_storage_key, execution_outcome_block_dependencies,
        manifest_version_identity_key, manifest_versions_contain_identity,
        validate_version_boundary, version_boundary_storage_key,
    },
    outcome::load_execution_outcomes_for_scope_internal,
    types::{
        ExecutionBoundaryInvalidation, ExecutionManifestInvalidation, ExecutionOutcome,
        ExecutionOutcomeInvalidationSummary,
    },
};

#[cfg(not(test))]
const REORG_INVALIDATION_BATCH_SIZE: i64 = 500;
#[cfg(test)]
const REORG_INVALIDATION_BATCH_SIZE: i64 = 2;

#[derive(Clone, Debug)]
struct ExecutionOutcomeReorgInvalidationCandidate {
    execution_cache_key: String,
    request_key: String,
    requested_chain_positions: Value,
    topology_version_boundary: Value,
    record_version_boundary: Value,
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
        if manifest_versions_contain_identity(
            &outcome.cache_key.manifest_versions,
            &outcome.cache_key.request_key,
            &target_identity,
        )? {
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
    let mut deleted_outcome_count = 0;
    let mut last_seen_cache_key = None;
    loop {
        let outcomes = load_execution_outcome_reorg_invalidation_candidate_batch_internal(
            &mut *transaction,
            last_seen_cache_key.as_deref(),
        )
        .await?;
        let Some(last_outcome) = outcomes.last() else {
            break;
        };
        last_seen_cache_key = Some(last_outcome.execution_cache_key.clone());

        let mut cache_keys = Vec::new();
        for outcome in outcomes {
            let dependencies = match execution_outcome_block_dependencies(
                &outcome.request_key,
                &outcome.requested_chain_positions,
                &outcome.topology_version_boundary,
                &outcome.record_version_boundary,
            ) {
                Ok(dependencies) => dependencies,
                Err(_) => {
                    cache_keys.push(outcome.execution_cache_key);
                    continue;
                }
            };
            if dependencies
                .iter()
                .any(|dependency| orphaned_blocks.contains(dependency))
            {
                cache_keys.push(outcome.execution_cache_key);
            }
        }

        deleted_outcome_count +=
            delete_execution_outcomes_by_keys(&mut transaction, &cache_keys).await?;
    }

    transaction
        .commit()
        .await
        .context("failed to commit execution reorg invalidation")?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count,
    })
}

impl ExecutionManifestInvalidation {
    fn identity_key(&self) -> String {
        manifest_version_identity_key(
            self.source_manifest_id,
            self.source_family.as_deref(),
            self.manifest_version,
        )
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

async fn load_execution_outcome_reorg_invalidation_candidate_batch_internal<'e, E>(
    executor: E,
    after_execution_cache_key: Option<&str>,
) -> Result<Vec<ExecutionOutcomeReorgInvalidationCandidate>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            execution_cache_key,
            request_key,
            requested_chain_positions,
            topology_version_boundary,
            record_version_boundary
        FROM execution_cache_outcomes
        WHERE request_type IN ('verified_resolution', 'verified_primary_name')
          AND ($1::text IS NULL OR execution_cache_key > $1)
        ORDER BY execution_cache_key
        LIMIT $2
        "#,
    )
    .bind(after_execution_cache_key)
    .bind(REORG_INVALIDATION_BATCH_SIZE)
    .fetch_all(executor)
    .await
    .context("failed to load execution outcome batch for reorg invalidation scope")?;

    rows.into_iter()
        .map(decode_execution_outcome_reorg_invalidation_candidate_row)
        .collect()
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
    if execution_cache_keys.is_empty() {
        return Ok(0);
    }

    let deleted_outcome_count = sqlx::query(
        r#"
        DELETE FROM execution_cache_outcomes
        WHERE execution_cache_key = ANY($1::text[])
        "#,
    )
    .bind(execution_cache_keys)
    .execute(&mut **executor)
    .await
    .context("failed to delete execution outcomes by cache key batch")?
    .rows_affected();

    Ok(deleted_outcome_count)
}

fn decode_execution_outcome_reorg_invalidation_candidate_row(
    row: PgRow,
) -> Result<ExecutionOutcomeReorgInvalidationCandidate> {
    Ok(ExecutionOutcomeReorgInvalidationCandidate {
        execution_cache_key: row
            .try_get("execution_cache_key")
            .context("execution outcome row missing execution_cache_key")?,
        request_key: row
            .try_get("request_key")
            .context("execution outcome row missing request_key")?,
        requested_chain_positions: row
            .try_get("requested_chain_positions")
            .context("execution outcome row missing requested_chain_positions")?,
        topology_version_boundary: row
            .try_get("topology_version_boundary")
            .context("execution outcome row missing topology_version_boundary")?,
        record_version_boundary: row
            .try_get("record_version_boundary")
            .context("execution outcome row missing record_version_boundary")?,
    })
}
