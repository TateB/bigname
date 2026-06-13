use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionBoundaryInvalidation, ExecutionManifestInvalidation,
    ExecutionOutcomeInvalidationSummary, invalidate_execution_outcomes_for_manifest_version,
    invalidate_execution_outcomes_for_manifest_version_and_request_key,
    invalidate_execution_outcomes_for_record_boundary,
    invalidate_execution_outcomes_for_record_boundary_and_request_key,
    invalidate_execution_outcomes_for_topology_boundary,
    invalidate_execution_outcomes_for_topology_boundary_and_request_key, normalize_evm_address,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";
const VERIFIED_PRIMARY_NAME_REQUEST_TYPE: &str =
    bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionManifestInvalidation {
    pub namespace: String,
    pub source_manifest_id: Option<i64>,
    pub source_family: Option<String>,
    pub manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionBoundaryInvalidation {
    pub namespace: String,
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub normalized_event_id: Option<i64>,
    pub event_kind: Option<String>,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPrimaryNameManifestInvalidation {
    pub namespace: String,
    pub address: String,
    pub coin_type: String,
    pub source_manifest_id: Option<i64>,
    pub source_family: Option<String>,
    pub manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPrimaryNameBoundaryInvalidation {
    pub namespace: String,
    pub address: String,
    pub coin_type: String,
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub normalized_event_id: Option<i64>,
    pub event_kind: Option<String>,
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: String,
}

pub async fn invalidate_verified_resolution_manifest_version(
    pool: &PgPool,
    invalidation: &VerifiedResolutionManifestInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_manifest_version(
        pool,
        &ExecutionManifestInvalidation {
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            source_manifest_id: invalidation.source_manifest_id,
            source_family: invalidation.source_family.clone(),
            manifest_version: invalidation.manifest_version,
        },
    )
    .await
}

pub async fn invalidate_verified_resolution_topology_boundary(
    pool: &PgPool,
    invalidation: &VerifiedResolutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_topology_boundary(
        pool,
        &ExecutionBoundaryInvalidation {
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            boundary: invalidation.boundary(),
        },
    )
    .await
}

pub async fn invalidate_verified_resolution_record_boundary(
    pool: &PgPool,
    invalidation: &VerifiedResolutionBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_record_boundary(
        pool,
        &ExecutionBoundaryInvalidation {
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            boundary: invalidation.boundary(),
        },
    )
    .await
}

pub async fn invalidate_verified_primary_name_manifest_version(
    pool: &PgPool,
    invalidation: &VerifiedPrimaryNameManifestInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_manifest_version_and_request_key(
        pool,
        &ExecutionManifestInvalidation {
            request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            source_manifest_id: invalidation.source_manifest_id,
            source_family: invalidation.source_family.clone(),
            manifest_version: invalidation.manifest_version,
        },
        &invalidation.request_key(),
    )
    .await
}

pub async fn invalidate_verified_primary_name_topology_boundary(
    pool: &PgPool,
    invalidation: &VerifiedPrimaryNameBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_topology_boundary_and_request_key(
        pool,
        &ExecutionBoundaryInvalidation {
            request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            boundary: invalidation.boundary(),
        },
        &invalidation.request_key(),
    )
    .await
}

pub async fn invalidate_verified_primary_name_record_boundary(
    pool: &PgPool,
    invalidation: &VerifiedPrimaryNameBoundaryInvalidation,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    invalidate_execution_outcomes_for_record_boundary_and_request_key(
        pool,
        &ExecutionBoundaryInvalidation {
            request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
            namespace: invalidation.namespace.clone(),
            boundary: invalidation.boundary(),
        },
        &invalidation.request_key(),
    )
    .await
}

pub async fn invalidate_verified_primary_name_claim_change(
    pool: &PgPool,
    namespace: &str,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    validate_claim_change_scope(namespace, request_key)?;
    let result = sqlx::query(
        r#"
        DELETE FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
          AND request_key = $3
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(request_key)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to invalidate verified primary-name outcome for namespace {namespace} request_key {request_key}"
        )
    })?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count: result.rows_affected(),
    })
}

pub async fn invalidate_verified_primary_name_claim_change_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: &str,
    request_key: &str,
) -> Result<ExecutionOutcomeInvalidationSummary> {
    validate_claim_change_scope(namespace, request_key)?;
    let result = sqlx::query(
        r#"
        DELETE FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
          AND request_key = $3
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(request_key)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to invalidate verified primary-name outcome for namespace {namespace} request_key {request_key}"
        )
    })?;

    Ok(ExecutionOutcomeInvalidationSummary {
        deleted_outcome_count: result.rows_affected(),
    })
}

impl VerifiedResolutionBoundaryInvalidation {
    fn boundary(&self) -> Value {
        json!({
            "logical_name_id": self.logical_name_id,
            "resource_id": self.resource_id,
            "normalized_event_id": self.normalized_event_id,
            "event_kind": self.event_kind,
            "chain_position": {
                "chain_id": self.chain_id,
                "block_number": self.block_number,
                "block_hash": self.block_hash,
                "timestamp": self.timestamp,
            }
        })
    }
}

impl VerifiedPrimaryNameManifestInvalidation {
    fn request_key(&self) -> String {
        verified_primary_name_request_key(&self.namespace, &self.address, &self.coin_type)
    }
}

impl VerifiedPrimaryNameBoundaryInvalidation {
    fn request_key(&self) -> String {
        verified_primary_name_request_key(&self.namespace, &self.address, &self.coin_type)
    }

    fn boundary(&self) -> Value {
        json!({
            "logical_name_id": self.logical_name_id,
            "resource_id": self.resource_id,
            "normalized_event_id": self.normalized_event_id,
            "event_kind": self.event_kind,
            "chain_position": {
                "chain_id": self.chain_id,
                "block_number": self.block_number,
                "block_hash": self.block_hash,
                "timestamp": self.timestamp,
            }
        })
    }
}

fn verified_primary_name_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
    format!("{namespace}:{}:{coin_type}", normalize_evm_address(address))
}

fn validate_claim_change_scope(namespace: &str, request_key: &str) -> Result<()> {
    if namespace.trim().is_empty() {
        bail!("verified primary-name claim invalidation namespace must not be blank");
    }
    if request_key.trim().is_empty() {
        bail!("verified primary-name claim invalidation request_key must not be blank");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
