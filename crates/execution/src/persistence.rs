use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, RawCallSnapshot, load_execution_outcome,
    load_execution_trace, load_primary_name_current, upsert_execution_outcome_in_transaction,
    upsert_execution_trace_in_transaction, upsert_raw_call_snapshots_in_transaction,
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    type VerifiedPrimaryPreTransactionHook = Arc<dyn Fn(Uuid) + Send + Sync + 'static>;

    static VERIFIED_PRIMARY_PRE_TRANSACTION_HOOK: Mutex<Option<VerifiedPrimaryPreTransactionHook>> =
        Mutex::new(None);

    pub(crate) struct VerifiedPrimaryPreTransactionHookGuard;

    impl Drop for VerifiedPrimaryPreTransactionHookGuard {
        fn drop(&mut self) {
            *VERIFIED_PRIMARY_PRE_TRANSACTION_HOOK
                .lock()
                .expect("verified-primary pre-transaction hook mutex poisoned") = None;
        }
    }

    pub(crate) fn install_verified_primary_pre_transaction_hook(
        hook: VerifiedPrimaryPreTransactionHook,
    ) -> VerifiedPrimaryPreTransactionHookGuard {
        *VERIFIED_PRIMARY_PRE_TRANSACTION_HOOK
            .lock()
            .expect("verified-primary pre-transaction hook mutex poisoned") = Some(hook);
        VerifiedPrimaryPreTransactionHookGuard
    }

    pub(super) fn run_verified_primary_pre_transaction_hook(execution_trace_id: Uuid) {
        let hook = VERIFIED_PRIMARY_PRE_TRANSACTION_HOOK
            .lock()
            .expect("verified-primary pre-transaction hook mutex poisoned")
            .clone();
        if let Some(hook) = hook {
            hook(execution_trace_id);
        }
    }
}

use crate::primary_name::{
    ensure_primary_name_anchor_matches, ensure_primary_name_anchor_matches_in_transaction,
    extract_verified_primary_readback_provenance, validate_verified_primary_request,
    validate_verified_primary_trace_and_outcome, verified_primary_context_label,
};
use crate::revalidation::revalidate_supported_resolution_persistence_from_storage;
use crate::validation::{validate_basenames_transport_direct_request, validate_direct_request};

/// One narrow direct-path ENS verified-resolution persistence request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistEnsExactNameVerifiedResolutionRequest {
    pub raw_call_snapshots: Vec<RawCallSnapshot>,
    pub trace: ExecutionTrace,
    pub outcome: ExecutionOutcome,
}

/// Persisted identity the route layer can read back through storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedVerifiedResolutionIdentity {
    pub execution_trace_id: Uuid,
    pub cache_key: ExecutionCacheKey,
}

/// One narrow ENS verified-primary persistence request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistEnsVerifiedPrimaryNameRequest {
    pub trace: ExecutionTrace,
    pub outcome: ExecutionOutcome,
}

/// Persisted verified-primary identity the route layer can read back through storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedVerifiedPrimaryNameIdentity {
    pub execution_trace_id: Uuid,
    pub cache_key: ExecutionCacheKey,
}

/// Additive verified-primary provenance material anchored to one persisted execution trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPrimaryNameReadbackProvenance {
    pub execution_trace_id: Uuid,
    pub manifest_versions: Value,
}

/// Persisted ENS verified-primary result plus the validated stored execution pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedEnsVerifiedPrimaryName {
    pub execution_trace_id: Uuid,
    pub cache_key: ExecutionCacheKey,
    pub verified_primary_name: Value,
    pub provenance: VerifiedPrimaryNameReadbackProvenance,
    pub trace: ExecutionTrace,
    pub outcome: ExecutionOutcome,
}

async fn hand_off_admitted_raw_call_snapshots_to_intake_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    raw_call_snapshots: &[RawCallSnapshot],
) -> Result<()> {
    if raw_call_snapshots.is_empty() {
        return Ok(());
    }

    upsert_raw_call_snapshots_in_transaction(transaction, raw_call_snapshots).await?;
    Ok(())
}

/// Persist one exact-name ENS verified-resolution supported-path result and return
/// the storage identity the route layer can load back.
pub async fn persist_ens_exact_name_verified_resolution_direct(
    pool: &PgPool,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<PersistedVerifiedResolutionIdentity> {
    validate_direct_request(request)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for ENS verified-resolution direct persistence")?;

    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
    if let Err(revalidation_error) =
        revalidate_supported_resolution_persistence_from_storage(&mut transaction, request).await
    {
        transaction.commit().await.context(
            "failed to commit ENS verified-resolution direct trace-only persistence after storage revalidation failure",
        )?;
        return Err(revalidation_error.context(
            "ENS verified-resolution direct supported outcome persistence failed closed after storage revalidation",
        ));
    }

    hand_off_admitted_raw_call_snapshots_to_intake_in_transaction(
        &mut transaction,
        &request.raw_call_snapshots,
    )
    .await?;

    let outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &request.outcome).await?;

    if trace.execution_trace_id != outcome.execution_trace_id {
        bail!(
            "persisted ENS verified-resolution direct path trace {} does not match outcome trace {}",
            trace.execution_trace_id,
            outcome.execution_trace_id
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "persisted ENS verified-resolution direct path request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit ENS verified-resolution direct persistence")?;

    Ok(PersistedVerifiedResolutionIdentity {
        execution_trace_id: trace.execution_trace_id,
        cache_key: outcome.cache_key,
    })
}

/// Persist one exact-name Basenames verified-resolution transport-assisted direct-path result and
/// return the storage identity the route layer can load back.
pub async fn persist_basenames_exact_name_verified_resolution_transport_direct(
    pool: &PgPool,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<PersistedVerifiedResolutionIdentity> {
    validate_basenames_transport_direct_request(request)?;

    let mut transaction = pool.begin().await.context(
        "failed to open transaction for Basenames verified-resolution transport-direct persistence",
    )?;

    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
    if let Err(revalidation_error) =
        revalidate_supported_resolution_persistence_from_storage(&mut transaction, request).await
    {
        transaction.commit().await.context(
            "failed to commit Basenames verified-resolution transport-direct trace-only persistence after storage revalidation failure",
        )?;
        return Err(revalidation_error.context(
            "Basenames verified-resolution transport-direct supported outcome persistence failed closed after storage revalidation",
        ));
    }

    let outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &request.outcome).await?;

    if trace.execution_trace_id != outcome.execution_trace_id {
        bail!(
            "persisted Basenames verified-resolution transport-direct trace {} does not match outcome trace {}",
            trace.execution_trace_id,
            outcome.execution_trace_id
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "persisted Basenames verified-resolution transport-direct request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit Basenames verified-resolution transport-direct persistence")?;

    Ok(PersistedVerifiedResolutionIdentity {
        execution_trace_id: trace.execution_trace_id,
        cache_key: outcome.cache_key,
    })
}

/// Persist one ENS verified-primary result for an exact `{address, namespace, coin_type}` tuple
/// and return the storage identity the route layer can load back.
pub async fn persist_ens_verified_primary_name(
    pool: &PgPool,
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<PersistedVerifiedPrimaryNameIdentity> {
    let validated = validate_verified_primary_request(request)?;
    let context = verified_primary_context_label(&validated.tuple.namespace)?;
    ensure_primary_name_anchor_matches(pool, &validated.tuple, &validated.verified_primary_name)
        .await?;
    #[cfg(test)]
    test_hooks::run_verified_primary_pre_transaction_hook(request.trace.execution_trace_id);

    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to open transaction for {context} persistence"))?;

    ensure_primary_name_anchor_matches_in_transaction(
        &mut transaction,
        &validated.tuple,
        &validated.verified_primary_name,
    )
    .await?;

    // Validation requires request_metadata.cache_identity to mirror the outcome cache key; the
    // trace write carries that full identity for API readback.
    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
    let outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &request.outcome).await?;
    // Storage normalizes the cache key before writing outcomes; the trace and
    // request_metadata must already match that canonical identity for readback.
    validate_verified_primary_trace_and_outcome(&trace, &outcome)?;

    if trace.execution_trace_id != outcome.execution_trace_id {
        bail!(
            "persisted {context} trace {} does not match outcome trace {}",
            trace.execution_trace_id,
            outcome.execution_trace_id
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "persisted {context} request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit {context} persistence"))?;

    Ok(PersistedVerifiedPrimaryNameIdentity {
        execution_trace_id: trace.execution_trace_id,
        cache_key: outcome.cache_key,
    })
}

/// Load one persisted ENS verified-primary answer by cache key. Readback remains gated by the
/// matching `primary_names_current(address, coin_type, namespace)` tuple anchor.
pub async fn load_persisted_ens_verified_primary_name(
    pool: &PgPool,
    cache_key: &ExecutionCacheKey,
) -> Result<Option<LoadedEnsVerifiedPrimaryName>> {
    let Some(outcome) = load_execution_outcome(pool, cache_key).await? else {
        return Ok(None);
    };
    let context = verified_primary_context_label(&outcome.namespace)?;

    let trace = load_execution_trace(pool, outcome.execution_trace_id)
        .await?
        .with_context(|| {
            format!(
                "failed to load persisted {context} trace {}",
                outcome.execution_trace_id
            )
        })?;

    let validated = validate_verified_primary_trace_and_outcome(&trace, &outcome)?;
    let provenance = extract_verified_primary_readback_provenance(&trace, &outcome)?;
    if load_primary_name_current(
        pool,
        &validated.tuple.normalized_address,
        &validated.tuple.namespace,
        &validated.tuple.coin_type,
    )
    .await?
    .is_none()
    {
        return Ok(None);
    }

    Ok(Some(LoadedEnsVerifiedPrimaryName {
        execution_trace_id: trace.execution_trace_id,
        cache_key: outcome.cache_key.clone(),
        verified_primary_name: validated.verified_primary_name.section,
        provenance,
        trace,
        outcome,
    }))
}
