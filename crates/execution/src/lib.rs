//! ENS verified-resolution exact-surface execution persistence bootstrap.

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, NameCurrentRow, RawCallSnapshot,
    RecordInventoryCurrentRow, SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey,
    SurfaceBindingKind, VerifiedResolutionPathClass, VerifiedResolutionSupportBoundary,
    load_primary_name_current, parse_supported_verified_resolution_record_key,
    upsert_execution_outcome_in_transaction, upsert_execution_trace_in_transaction,
    upsert_raw_call_snapshots_in_transaction,
};
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

#[cfg(test)]
use bigname_storage::{
    ChainLineageBlock, NameSurface, PrimaryNameClaimStatus, PrimaryNameCurrentRow, Resource,
    SurfaceBinding, TokenLineage, upsert_chain_lineage_blocks, upsert_execution_outcome,
    upsert_execution_trace, upsert_name_current_rows, upsert_name_surfaces,
    upsert_primary_name_current_rows, upsert_record_inventory_current_rows, upsert_resources,
    upsert_surface_bindings, upsert_token_lineages,
};

pub use bigname_storage::{
    CanonicalityState, ExecutionTraceStep, load_execution_outcome, load_execution_trace,
    load_raw_call_snapshots_by_block_hash,
};

pub const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";
pub const VERIFIED_PRIMARY_NAME_REQUEST_TYPE: &str = "verified_primary_name";
pub const ENS_NAMESPACE: &str = bigname_storage::ENS_NAMESPACE;
pub const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
pub const BASE_MAINNET_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = "ens_execution";
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const ENS_UNIVERSAL_RESOLVER_ADDRESS: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
pub const BASENAMES_EXECUTION_SOURCE_FAMILY: &str = "basenames_execution";
pub const BASENAMES_L1_RESOLVER_ROLE: &str = "l1_resolver";
pub const BASENAMES_L1_RESOLVER_ADDRESS: &str = bigname_storage::BASENAMES_L1_RESOLVER_ADDRESS;
pub const DECLARED_REGISTRY_PATH_BINDING_KIND: &str = "declared_registry_path";
pub const LINKED_SUBREGISTRY_PATH_BINDING_KIND: &str = "linked_subregistry_path";
pub const RESOLVER_ALIAS_PATH_BINDING_KIND: &str = "resolver_alias_path";
pub const OBSERVED_WILDCARD_PATH_BINDING_KIND: &str = "observed_wildcard_path";
pub const MIGRATION_REBIND_BINDING_KIND: &str = "migration_rebind";
pub const OBSERVED_ONLY_BINDING_KIND: &str = "observed_only";

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

/// Current execution bootstrap status.
pub const fn bootstrap_status() -> &'static str {
    "ens-direct-and-basenames-transport-verified-resolution-producer-ready"
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
    ensure_primary_name_anchor_exists(pool, &validated.tuple).await?;

    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to open transaction for {context} persistence"))?;

    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
    let outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &request.outcome).await?;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerifiedQueryStatus {
    Success,
    NotFound,
    ExecutionFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerifiedPrimaryNameStatus {
    Success,
    NotFound,
    Mismatch,
    InvalidName,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedPrimaryNameTuple {
    namespace: String,
    normalized_address: String,
    coin_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedPrimaryNameSection {
    section: Value,
    status: VerifiedPrimaryNameStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedVerifiedPrimaryName {
    tuple: VerifiedPrimaryNameTuple,
    verified_primary_name: VerifiedPrimaryNameSection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedQuerySummary {
    record_key: String,
    selector: SupportedVerifiedRecordKey,
    status: VerifiedQueryStatus,
    value: Option<String>,
    failure_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestedSelectorSet {
    surface: String,
    ordered_record_keys: Vec<String>,
    binding_kind: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestedChainPosition {
    chain_id: String,
    block_number: i64,
    block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ManifestVersionIdentity {
    source_manifest_id: Option<i64>,
    source_family: Option<String>,
    manifest_version: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupportedResolutionPathClass {
    Direct,
    AliasOnly,
    WildcardDerived,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SupportedResolutionStepSummary {
    saw_universal_resolver_call: bool,
    saw_alias_step: bool,
}

fn verified_primary_context_label(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok("ENS verified-primary"),
        BASENAMES_NAMESPACE => Ok("Basenames verified-primary"),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}

fn verified_primary_execution_source_family(namespace: &str) -> Result<&'static str> {
    match namespace {
        ENS_NAMESPACE => Ok(ENS_EXECUTION_SOURCE_FAMILY),
        BASENAMES_NAMESPACE => Ok(BASENAMES_EXECUTION_SOURCE_FAMILY),
        other => bail!("verified-primary namespace {other} is unsupported"),
    }
}

fn validate_verified_primary_request(
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(&request.trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        request.outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(
        &request.trace,
        &request.outcome,
        &tuple,
        &verified_primary_name,
    )?;
    validate_verified_primary_outcome(
        &request.outcome,
        &request.trace,
        &tuple,
        &verified_primary_name,
    )?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}

fn validate_verified_primary_trace_and_outcome(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(trace, outcome, &tuple, &verified_primary_name)?;
    validate_verified_primary_outcome(outcome, trace, &tuple, &verified_primary_name)?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}

fn extract_verified_primary_readback_provenance(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<VerifiedPrimaryNameReadbackProvenance> {
    let context = verified_primary_context_label(&trace.namespace)?;
    let cache_manifest_versions = required_array(
        Some(&outcome.cache_key.manifest_versions),
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let trace_manifest_versions = required_array(
            Some(trace_manifest_versions),
            &format!("{context} trace.manifest_context.manifest_versions"),
        )?;
        if trace_manifest_versions != cache_manifest_versions {
            bail!(
                "{context} trace.manifest_context.manifest_versions must match cache_key.manifest_versions"
            );
        }
    }

    Ok(VerifiedPrimaryNameReadbackProvenance {
        execution_trace_id: trace.execution_trace_id,
        manifest_versions: Value::Array(cache_manifest_versions.clone()),
    })
}

fn validate_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    validate_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    validate_outcome(&request.outcome, &request.trace, &queries)?;
    validate_raw_call_snapshots(
        &request.raw_call_snapshots,
        &request.outcome,
        &requested_selectors,
    )?;
    Ok(queries)
}

fn validate_basenames_transport_direct_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<Vec<VerifiedQuerySummary>> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    ensure_requested_selectors_match_queries(&requested_selectors, &queries)?;
    validate_basenames_transport_direct_trace(
        &request.trace,
        &request.outcome,
        &requested_selectors,
        &queries,
    )?;
    validate_basenames_transport_direct_outcome(
        &request.outcome,
        &request.trace,
        &requested_selectors,
        &queries,
    )?;
    if !request.raw_call_snapshots.is_empty() {
        bail!(
            "Basenames transport-assisted direct persistence does not admit raw_call_snapshots yet"
        );
    }
    Ok(queries)
}

fn validate_basenames_transport_direct_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "Basenames transport-direct verified resolution trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if trace.namespace != BASENAMES_NAMESPACE {
        bail!(
            "Basenames transport-direct verified resolution trace {} must use namespace {}",
            trace.execution_trace_id,
            BASENAMES_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "Basenames transport-direct verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key = normalized_request_key(
        BASENAMES_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "Basenames transport-direct verified resolution trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;
    ensure_basenames_requested_positions(
        &requested_positions,
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "Basenames transport-direct verified resolution trace.gateway_digests",
    )?;
    if gateway_digests.is_empty() {
        bail!(
            "Basenames transport-direct verified resolution must record gateway_digests for CCIP readback"
        );
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        BASENAMES_EXECUTION_SOURCE_FAMILY,
        "Basenames transport-direct verified resolution",
    )? {
        bail!(
            "Basenames transport-direct verified resolution must include source_family {} in manifest context or cache key",
            BASENAMES_EXECUTION_SOURCE_FAMILY
        );
    }

    ensure_contains_basenames_l1_resolver_call(
        &trace.contracts_called,
        trace.execution_trace_id,
        "Basenames transport-direct verified resolution",
    )?;
    ensure_steps_are_supported_basenames_transport_direct_path(
        trace,
        requested_selectors,
        trace.execution_trace_id,
    )?;
    validate_trace_terminal_payloads(trace, queries)?;

    Ok(())
}

fn validate_basenames_transport_direct_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if outcome.namespace != BASENAMES_NAMESPACE {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            BASENAMES_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "Basenames transport-direct verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "Basenames transport-direct verified resolution trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "Basenames transport-direct verified resolution outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    let expected_request_key = normalized_request_key(
        BASENAMES_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if outcome.cache_key.request_key != expected_request_key {
        bail!(
            "Basenames transport-direct verified resolution outcome request_key {} does not match expected {}",
            outcome.cache_key.request_key,
            expected_request_key
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "Basenames transport-direct verified resolution outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "Basenames transport-direct verified resolution cache_key.requested_chain_positions",
    )?;
    ensure_basenames_requested_positions(
        &requested_positions,
        "Basenames transport-direct verified resolution cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "Basenames transport-direct verified resolution trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "Basenames transport-direct verified resolution trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    if queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed)
    {
        required_object(
            outcome.failure_payload.as_ref(),
            "Basenames transport-direct verified resolution execution_failed outcome.failure_payload",
        )?;
    } else if outcome.failure_payload.is_some() {
        bail!(
            "Basenames transport-direct verified resolution outcome for request_key {} must not set failure_payload unless every selector status is execution_failed",
            outcome.cache_key.request_key
        );
    }

    Ok(())
}

async fn revalidate_supported_resolution_persistence_from_storage(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<()> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let queries = extract_supported_verified_queries(&request.outcome)?;
    let logical_name_id = format!(
        "{}:{}",
        request.trace.namespace, requested_selectors.surface
    );
    let context = match request.trace.namespace.as_str() {
        ENS_NAMESPACE => "ENS verified-resolution storage revalidation",
        BASENAMES_NAMESPACE => "Basenames verified-resolution storage revalidation",
        other => bail!("{other} verified-resolution storage revalidation is unsupported"),
    };

    let row = load_name_current_for_revalidation(transaction, &logical_name_id)
        .await?
        .with_context(|| {
            format!("{context} requires name_current row for logical_name_id {logical_name_id}")
        })?;
    let record_inventory_row =
        load_supported_record_inventory_current_for_revalidation(transaction, &row)
            .await
            .with_context(|| {
                format!(
                    "{context} failed to load supported record_inventory_current for logical_name_id {logical_name_id}"
                )
            })?;

    let stored_manifest_versions = normalize_manifest_versions_for_revalidation(
        row.provenance
            .as_object()
            .and_then(|object| object.get("manifest_versions"))
            .with_context(|| {
                format!("{context} name_current provenance must include manifest_versions")
            })?,
        &format!("{context} name_current provenance.manifest_versions"),
    )?;
    let outcome_manifest_versions = normalize_manifest_versions_for_revalidation(
        &request.outcome.cache_key.manifest_versions,
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if stored_manifest_versions != outcome_manifest_versions {
        bail!(
            "{context} cache_key.manifest_versions must match name_current provenance.manifest_versions"
        );
    }

    let stored_requested_positions =
        build_requested_chain_positions_from_projection(&row.chain_positions)?;
    let outcome_requested_positions = normalize_requested_chain_positions(
        Some(&request.outcome.cache_key.requested_chain_positions),
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    if stored_requested_positions != outcome_requested_positions {
        bail!(
            "{context} cache_key.requested_chain_positions must match projected chain_positions for logical_name_id {logical_name_id}"
        );
    }

    let topology = build_resolution_topology_for_revalidation(&row, record_inventory_row.as_ref())?;
    let support_boundary = bigname_storage::try_resolution_verified_support_boundary(
        &row,
        record_inventory_row.as_ref(),
    )?
    .with_context(|| {
        format!(
            "{context} could not re-establish a supported mixed-route topology boundary for logical_name_id {logical_name_id}"
        )
    })?;

    ensure_storage_supported_boundary_matches_request(
        request,
        &requested_selectors,
        &topology,
        &support_boundary,
        context,
    )?;
    ensure_storage_selector_families_supported(
        record_inventory_row.as_ref(),
        &queries,
        &request.outcome.cache_key.request_key,
        context,
    )?;

    Ok(())
}

async fn load_supported_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let Some((resource_id, record_version_boundary)) =
        bigname_storage::resolution_record_inventory_lookup_key_for_revalidation(row)?
    else {
        return Ok(None);
    };

    if let Some(record_inventory_row) = load_record_inventory_current_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) = find_supported_record_inventory_boundary_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current_for_revalidation(transaction, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

fn normalize_requested_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let mut positions = required_chain_positions(value, context)?;
    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

fn build_requested_chain_positions_from_projection(
    chain_positions: &Value,
) -> Result<Vec<RequestedChainPosition>> {
    Ok(
        bigname_storage::resolution_requested_chain_positions_from_projection(chain_positions)?
            .into_iter()
            .map(|position| RequestedChainPosition {
                chain_id: position.chain_id,
                block_number: position.block_number,
                block_hash: position.block_hash,
            })
            .collect(),
    )
}

fn normalize_manifest_versions_for_revalidation(value: &Value, context: &str) -> Result<Value> {
    let items = value
        .as_array()
        .with_context(|| format!("{context} must be a JSON array"))?;
    if items.is_empty() {
        bail!("{context} must not be empty");
    }

    let mut versions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = item
            .as_object()
            .with_context(|| format!("{context}[{index}] must be a JSON object"))?;
        let source_manifest_id = match object.get("source_manifest_id") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
                format!("{context}[{index}].source_manifest_id must be null or a positive integer")
            })?),
        };
        let source_family = match object.get("source_family") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
            Some(_) => bail!("{context}[{index}].source_family must be null or a non-empty string"),
        };
        if source_manifest_id.is_none() && source_family.is_none() {
            bail!("{context}[{index}] must include source_manifest_id or source_family");
        }
        let manifest_version = object
            .get("manifest_version")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .with_context(|| {
                format!("{context}[{index}].manifest_version must be a positive integer")
            })?;
        versions.push(ManifestVersionIdentity {
            source_manifest_id,
            source_family,
            manifest_version,
        });
    }

    versions.sort();
    versions.dedup();

    Ok(Value::Array(
        versions
            .into_iter()
            .map(|version| {
                let mut object = Map::new();
                if let Some(source_manifest_id) = version.source_manifest_id {
                    object.insert(
                        "source_manifest_id".to_owned(),
                        Value::Number(source_manifest_id.into()),
                    );
                }
                if let Some(source_family) = version.source_family {
                    object.insert("source_family".to_owned(), Value::String(source_family));
                }
                object.insert(
                    "manifest_version".to_owned(),
                    Value::Number(version.manifest_version.into()),
                );
                Value::Object(object)
            })
            .collect(),
    ))
}

fn build_resolution_topology_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Value> {
    if let Some(projected_topology) =
        bigname_storage::projected_resolution_topology(&row.declared_summary)
    {
        return Ok(projected_topology);
    }

    build_legacy_resolution_topology_for_revalidation(row, record_inventory_row)
}

fn build_legacy_resolution_topology_for_revalidation(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Result<Value> {
    if !matches!(row.namespace.as_str(), ENS_NAMESPACE | BASENAMES_NAMESPACE)
        || row.binding_kind != Some(SurfaceBindingKind::DeclaredRegistryPath)
        || row.resource_id.is_none()
    {
        bail!("declared resolution topology is not yet projected");
    }

    let resolver_summary = json_field(&row.declared_summary, "resolver")
        .filter(|value| value.is_object())
        .filter(|value| !summary_is_unsupported(Some(value)))
        .with_context(|| "declared resolution topology is not yet projected".to_owned())?;

    let resolver_chain_id = json_string_field(json_field(resolver_summary, "chain_id"));
    let resolver_address = json_string_field(json_field(resolver_summary, "address"));
    if resolver_chain_id.is_some() != resolver_address.is_some() {
        bail!("declared resolution topology is not yet projected");
    }

    let record_version_boundary =
        bigname_storage::resolution_record_version_boundary_for_revalidation(
            row,
            record_inventory_row,
        )
        .with_context(|| "declared resolution topology is not yet projected".to_owned())?;

    let registry_ref = build_resolution_name_ref_for_revalidation(row);
    let resolver_hop = build_resolution_resolver_hop_for_revalidation(
        row,
        resolver_chain_id,
        resolver_address,
        json_string_field(json_field(resolver_summary, "latest_event_kind")),
    );

    let mut version_boundaries = Map::new();
    version_boundaries.insert(
        "topology_version_boundary".to_owned(),
        record_version_boundary.clone(),
    );
    version_boundaries.insert(
        "record_version_boundary".to_owned(),
        record_version_boundary,
    );

    let mut topology = Map::new();
    topology.insert("registry_path".to_owned(), Value::Array(vec![registry_ref]));
    topology.insert("subregistry_path".to_owned(), Value::Array(Vec::new()));
    topology.insert("resolver_path".to_owned(), Value::Array(vec![resolver_hop]));
    topology.insert(
        "wildcard".to_owned(),
        Value::Object(default_wildcard_detail()),
    );
    topology.insert("alias".to_owned(), Value::Object(default_alias_detail()));
    topology.insert(
        "version_boundaries".to_owned(),
        Value::Object(version_boundaries),
    );
    topology.insert(
        "transport".to_owned(),
        Value::Object(build_resolution_transport_for_revalidation(row)),
    );
    Ok(Value::Object(topology))
}

fn build_resolution_name_ref_for_revalidation(row: &NameCurrentRow) -> Value {
    let mut name_ref = Map::new();
    name_ref.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    name_ref.insert("namespace".to_owned(), Value::String(row.namespace.clone()));
    name_ref.insert(
        "normalized_name".to_owned(),
        Value::String(row.normalized_name.clone()),
    );
    name_ref.insert(
        "canonical_display_name".to_owned(),
        Value::String(row.canonical_display_name.clone()),
    );
    name_ref.insert("namehash".to_owned(), Value::String(row.namehash.clone()));
    name_ref.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    name_ref.insert(
        "binding_kind".to_owned(),
        row.binding_kind
            .map(|value| Value::String(value.as_str().to_owned()))
            .unwrap_or(Value::Null),
    );
    Value::Object(name_ref)
}

fn build_resolution_resolver_hop_for_revalidation(
    row: &NameCurrentRow,
    chain_id: Option<String>,
    address: Option<String>,
    latest_event_kind: Option<String>,
) -> Value {
    let mut hop = Map::new();
    hop.insert(
        "logical_name_id".to_owned(),
        Value::String(row.logical_name_id.clone()),
    );
    hop.insert("namespace".to_owned(), Value::String(row.namespace.clone()));
    hop.insert(
        "normalized_name".to_owned(),
        Value::String(row.normalized_name.clone()),
    );
    hop.insert(
        "canonical_display_name".to_owned(),
        Value::String(row.canonical_display_name.clone()),
    );
    hop.insert(
        "resource_id".to_owned(),
        row.resource_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
    );
    hop.insert(
        "chain_id".to_owned(),
        chain_id.map(Value::String).unwrap_or(Value::Null),
    );
    hop.insert(
        "address".to_owned(),
        address.map(Value::String).unwrap_or(Value::Null),
    );
    hop.insert(
        "latest_event_kind".to_owned(),
        latest_event_kind.map(Value::String).unwrap_or(Value::Null),
    );
    Value::Object(hop)
}

fn build_resolution_transport_for_revalidation(row: &NameCurrentRow) -> Map<String, Value> {
    let mut transport = Map::new();
    if row.namespace == BASENAMES_NAMESPACE {
        transport.insert(
            "source_chain_id".to_owned(),
            Value::String(BASE_MAINNET_CHAIN_ID.to_owned()),
        );
        transport.insert(
            "target_chain_id".to_owned(),
            Value::String(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
        );
        transport.insert(
            "contract_address".to_owned(),
            Value::String(BASENAMES_L1_RESOLVER_ADDRESS.to_owned()),
        );
        transport.insert("latest_event_kind".to_owned(), Value::Null);
        return transport;
    }

    transport.insert("source_chain_id".to_owned(), Value::Null);
    transport.insert("target_chain_id".to_owned(), Value::Null);
    transport.insert("contract_address".to_owned(), Value::Null);
    transport.insert("latest_event_kind".to_owned(), Value::Null);
    transport
}

fn ensure_storage_supported_boundary_matches_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    requested_selectors: &RequestedSelectorSet,
    topology: &Value,
    support_boundary: &VerifiedResolutionSupportBoundary,
    context: &str,
) -> Result<()> {
    let expected_path_class = match request.trace.namespace.as_str() {
        ENS_NAMESPACE => match classify_supported_resolution_path(
            requested_selectors.binding_kind.as_deref(),
            request.trace.execution_trace_id,
        )? {
            SupportedResolutionPathClass::Direct => VerifiedResolutionPathClass::Direct,
            SupportedResolutionPathClass::AliasOnly => VerifiedResolutionPathClass::AliasOnly,
            SupportedResolutionPathClass::WildcardDerived => {
                VerifiedResolutionPathClass::WildcardDerived
            }
        },
        BASENAMES_NAMESPACE => VerifiedResolutionPathClass::BasenamesTransportDirect,
        other => bail!("{context} does not support namespace {other}"),
    };
    if support_boundary.path_class != expected_path_class {
        bail!("{context} stored supported path class does not match the request trace");
    }
    if support_boundary.topology_version_boundary
        != request.outcome.cache_key.topology_version_boundary
    {
        bail!(
            "{context} cache_key.topology_version_boundary must match the stored mixed-route topology boundary"
        );
    }
    if support_boundary.record_version_boundary != request.outcome.cache_key.record_version_boundary
    {
        bail!(
            "{context} cache_key.record_version_boundary must match the stored mixed-route record boundary"
        );
    }

    let stored_alias =
        normalize_alias_detail(json_field(topology, "alias"), &request.trace.namespace)?;
    let request_alias = normalize_alias_detail(
        persisted_trace_detail_object(&request.trace, "alias").as_ref(),
        &request.trace.namespace,
    )?;
    if stored_alias != request_alias {
        bail!("{context} stored alias topology does not match the request trace");
    }

    let stored_wildcard =
        normalize_wildcard_detail(json_field(topology, "wildcard"), &request.trace.namespace)?;
    let request_wildcard = normalize_wildcard_detail(
        persisted_trace_detail_object(&request.trace, "wildcard").as_ref(),
        &request.trace.namespace,
    )?;
    if stored_wildcard != request_wildcard {
        bail!("{context} stored wildcard topology does not match the request trace");
    }

    let stored_transport = normalize_transport_detail(json_field(topology, "transport"))?;
    let request_transport = normalize_transport_detail(
        persisted_trace_detail_object(&request.trace, "transport").as_ref(),
    )?;
    if stored_transport != request_transport {
        bail!("{context} stored transport topology does not match the request trace");
    }

    Ok(())
}

fn normalize_alias_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(alias) = value else {
        return Ok(Value::Object(default_alias_detail()));
    };
    let alias = alias
        .as_object()
        .with_context(|| "alias detail must be a JSON object".to_owned())?;
    let mut normalized = default_alias_detail();

    let final_target = match alias.get("final_target") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "alias.final_target", namespace)?;
            value.clone()
        }
    };
    let hops = alias
        .get("hops")
        .and_then(Value::as_array)
        .with_context(|| "alias.hops must be a JSON array".to_owned())?;
    for (index, hop) in hops.iter().enumerate() {
        validate_verified_primary_name_ref(Some(hop), &format!("alias.hops[{index}]"), namespace)?;
    }
    if final_target.is_null() != hops.is_empty() {
        bail!("alias detail must set final_target and non-empty hops together");
    }
    normalized.insert("final_target".to_owned(), final_target);
    normalized.insert("hops".to_owned(), Value::Array(hops.clone()));
    Ok(Value::Object(normalized))
}

fn normalize_wildcard_detail(value: Option<&Value>, namespace: &str) -> Result<Value> {
    let Some(wildcard) = value else {
        return Ok(Value::Object(default_wildcard_detail()));
    };
    let wildcard = wildcard
        .as_object()
        .with_context(|| "wildcard detail must be a JSON object".to_owned())?;
    let mut normalized = default_wildcard_detail();

    let source = match wildcard.get("source") {
        None | Some(Value::Null) => Value::Null,
        Some(value) => {
            validate_verified_primary_name_ref(Some(value), "wildcard.source", namespace)?;
            value.clone()
        }
    };
    let matched_labels = wildcard
        .get("matched_labels")
        .and_then(Value::as_array)
        .with_context(|| "wildcard.matched_labels must be a JSON array".to_owned())?;
    if source.is_null() && !matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels empty when source is null");
    }
    if !source.is_null() && matched_labels.is_empty() {
        bail!("wildcard detail must keep matched_labels non-empty when source is present");
    }
    normalized.insert("source".to_owned(), source);
    normalized.insert(
        "matched_labels".to_owned(),
        Value::Array(matched_labels.clone()),
    );
    Ok(Value::Object(normalized))
}

fn normalize_transport_detail(value: Option<&Value>) -> Result<Value> {
    let Some(transport) = value else {
        return Ok(Value::Object(default_transport_detail()));
    };
    let transport = transport
        .as_object()
        .with_context(|| "transport detail must be a JSON object".to_owned())?;
    let mut normalized = default_transport_detail();
    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        let value = match transport.get(field_name) {
            None | Some(Value::Null) => Value::Null,
            Some(Value::String(value)) if !value.trim().is_empty() => Value::String(value.clone()),
            Some(_) => {
                bail!("transport detail field {field_name} must be null or a non-empty string")
            }
        };
        normalized.insert(field_name.to_owned(), value);
    }
    Ok(Value::Object(normalized))
}

fn default_alias_detail() -> Map<String, Value> {
    let mut alias = Map::new();
    alias.insert("final_target".to_owned(), Value::Null);
    alias.insert("hops".to_owned(), Value::Array(Vec::new()));
    alias
}

fn default_wildcard_detail() -> Map<String, Value> {
    let mut wildcard = Map::new();
    wildcard.insert("source".to_owned(), Value::Null);
    wildcard.insert("matched_labels".to_owned(), Value::Array(Vec::new()));
    wildcard
}

fn default_transport_detail() -> Map<String, Value> {
    let mut transport = Map::new();
    transport.insert("source_chain_id".to_owned(), Value::Null);
    transport.insert("target_chain_id".to_owned(), Value::Null);
    transport.insert("contract_address".to_owned(), Value::Null);
    transport.insert("latest_event_kind".to_owned(), Value::Null);
    transport
}

fn ensure_storage_selector_families_supported(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    queries: &[VerifiedQuerySummary],
    request_key: &str,
    context: &str,
) -> Result<()> {
    let record_inventory_row = record_inventory_row.with_context(|| {
        format!(
            "{context} requires record_inventory_current to revalidate supported selectors for request_key {request_key}"
        )
    })?;
    let unsupported_families = record_inventory_row
        .unsupported_families
        .as_array()
        .with_context(|| {
            format!("{context} record_inventory_current.unsupported_families must be a JSON array")
        })?;
    let entries = record_inventory_row.entries.as_array().with_context(|| {
        format!("{context} record_inventory_current.entries must be a JSON array")
    })?;

    for query in queries {
        let (record_family, selector_key) =
            selector_family_and_key(&query.record_key, &query.selector);

        if unsupported_families.iter().any(|entry| {
            json_string_field(json_field(entry, "record_family"))
                .is_some_and(|value| value == record_family)
        }) {
            bail!(
                "{context} record family {record_family} is still unsupported in record_inventory_current for request_key {request_key}"
            );
        }

        if entries.iter().any(|entry| {
            json_string_field(json_field(entry, "record_key"))
                .is_some_and(|value| value == query.record_key)
                && json_string_field(json_field(entry, "status"))
                    .is_some_and(|value| value == "unsupported")
                && selector_key_matches_inventory(entry, selector_key.as_deref())
        }) {
            bail!(
                "{context} selector {} is still unsupported in record_inventory_current for request_key {request_key}",
                query.record_key
            );
        }
    }

    Ok(())
}

fn selector_family_and_key(
    record_key: &str,
    selector: &SupportedVerifiedRecordKey,
) -> (String, Option<String>) {
    match selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            ("addr".to_owned(), Some(coin_type.clone()))
        }
        SupportedVerifiedRecordKey::Avatar => ("avatar".to_owned(), None),
        SupportedVerifiedRecordKey::Contenthash => ("contenthash".to_owned(), None),
        SupportedVerifiedRecordKey::Text => (
            "text".to_owned(),
            record_key.strip_prefix("text:").map(str::to_owned),
        ),
    }
}

fn selector_key_matches_inventory(entry: &Value, selector_key: Option<&str>) -> bool {
    match (json_field(entry, "selector_key"), selector_key) {
        (None | Some(Value::Null), None) => true,
        (Some(Value::String(left)), Some(right)) => left == right,
        _ => false,
    }
}

async fn find_supported_record_inventory_boundary_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<Value>> {
    let logical_name_id =
        json_string_field(json_field(record_version_boundary, "logical_name_id")).with_context(
            || {
                format!(
                    "supported record version boundary for resource_id {resource_id} must include logical_name_id"
                )
            },
        )?;
    let chain_position = json_field(record_version_boundary, "chain_position").with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position"
        )
    })?;
    let chain_id = json_string_field(json_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = json_field(chain_position, "block_number")
        .and_then(Value::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = json_string_field(json_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = json_string_field(json_field(chain_position, "timestamp")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.timestamp"
        )
    })?;

    let boundaries = sqlx::query(
        r#"
        SELECT record_version_boundary
        FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary ->> 'logical_name_id' = $2
          AND record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
        ORDER BY
          (record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    )
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to locate supported record_inventory_current boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        row.try_get("record_version_boundary").with_context(|| {
            format!(
                "supported record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary"
            )
        })
    })
    .collect::<Result<Vec<Value>, _>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    if let Some(second_boundary) = boundaries.get(1)
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}

fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    bigname_storage::record_version_boundary_has_pointer(record_version_boundary)
}

async fn load_name_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = $1
          AND surface.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND (
              nc.surface_binding_id IS NULL
              OR (
                  resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND binding.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND (
                      nc.token_lineage_id IS NULL
                      OR token_lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
              )
          )
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row_for_revalidation)
        .transpose()
}

fn decode_name_current_row_for_revalidation(row: PgRow) -> Result<NameCurrentRow> {
    Ok(NameCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind: row
            .try_get::<Option<String>, _>("binding_kind")
            .context("missing binding_kind")?
            .map(|value| parse_surface_binding_kind_for_revalidation(&value))
            .transpose()?,
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind_for_revalidation(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}

async fn load_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = serde_json::to_string(record_version_boundary)
        .context("failed to serialize revalidation record_version_boundary")?;

    let row = sqlx::query(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary = $2::JSONB
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary_key)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row_for_revalidation)
        .transpose()
}

fn decode_record_inventory_current_row_for_revalidation(
    row: PgRow,
) -> Result<RecordInventoryCurrentRow> {
    Ok(RecordInventoryCurrentRow {
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        record_version_boundary: row
            .try_get("record_version_boundary")
            .context("missing record_version_boundary")?,
        enumeration_basis: row
            .try_get("enumeration_basis")
            .context("missing enumeration_basis")?,
        selectors: row.try_get("selectors").context("missing selectors")?,
        explicit_gaps: row
            .try_get("explicit_gaps")
            .context("missing explicit_gaps")?,
        unsupported_families: row
            .try_get("unsupported_families")
            .context("missing unsupported_families")?,
        last_change: row.try_get("last_change").context("missing last_change")?,
        entries: row.try_get("entries").context("missing entries")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn summary_is_unsupported(section: Option<&Value>) -> bool {
    matches!(
        json_string_field(section.and_then(|value| json_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && json_string_field(section.and_then(|value| json_field(value, "unsupported_reason")))
        .is_some()
}

fn json_field<'a>(value: &'a Value, field_name: &str) -> Option<&'a Value> {
    value.as_object()?.get(field_name)
}

fn json_string_field(value: Option<&Value>) -> Option<String> {
    value?.as_str().map(str::to_owned)
}

fn extract_verified_primary_tuple(trace: &ExecutionTrace) -> Result<VerifiedPrimaryNameTuple> {
    let trace_namespace = trace.namespace.as_str();
    let context = verified_primary_context_label(trace_namespace)?;
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        &format!("{context} trace.request_metadata"),
    )?;
    let normalized_address = required_string(
        request_metadata,
        "normalized_address",
        &format!("{context} trace.request_metadata"),
    )?
    .to_owned();
    if normalized_address != normalize_address(&normalized_address) {
        bail!("{context} trace.request_metadata.normalized_address must already be lowercase");
    }

    let coin_type = required_coin_type_field(
        request_metadata,
        "coin_type",
        &format!("{context} trace.request_metadata"),
    )?;
    let namespace = if let Some(namespace) = optional_nonempty_string_field(
        request_metadata,
        "namespace",
        &format!("{context} trace.request_metadata"),
    )? {
        if namespace != trace_namespace {
            bail!(
                "{context} trace.request_metadata.namespace must be {}",
                trace_namespace
            );
        }
        namespace.to_owned()
    } else {
        trace.namespace.clone()
    };

    Ok(VerifiedPrimaryNameTuple {
        namespace,
        normalized_address,
        coin_type,
    })
}

fn extract_verified_primary_name_section(
    payload: Option<&Value>,
    context: &str,
    namespace: &str,
) -> Result<VerifiedPrimaryNameSection> {
    let payload = required_object(payload, context)?;
    ensure_only_allowed_fields(payload, &["verified_primary_name"], context)?;

    let section_context = format!("{context}.verified_primary_name");
    let section = required_object(payload.get("verified_primary_name"), &section_context)?;
    ensure_only_allowed_fields(
        section,
        &["status", "name", "failure_reason"],
        &section_context,
    )?;

    let status = match required_string(section, "status", &section_context)? {
        "success" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            ensure_absent(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Success
        }
        "not_found" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::NotFound
        }
        "mismatch" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Mismatch
        }
        "invalid_name" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::InvalidName
        }
        "execution_failed" => {
            ensure_absent(section, "name", &section_context)?;
            required_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::ExecutionFailed
        }
        status => bail!(
            "verified-primary only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
        ),
    };

    Ok(VerifiedPrimaryNameSection {
        section: Value::Object(section.clone()),
        status,
    })
}

fn validate_verified_primary_name_ref(
    value: Option<&Value>,
    context: &str,
    expected_namespace: &str,
) -> Result<()> {
    let name = required_object(value, context)?;
    ensure_only_allowed_fields(
        name,
        &[
            "logical_name_id",
            "namespace",
            "normalized_name",
            "canonical_display_name",
            "namehash",
            "resource_id",
            "binding_kind",
        ],
        context,
    )?;

    let logical_name_id = required_string(name, "logical_name_id", context)?;
    let namespace = required_string(name, "namespace", context)?;
    let normalized_name = required_string(name, "normalized_name", context)?;
    required_string(name, "canonical_display_name", context)?;
    required_string(name, "namehash", context)?;
    optional_nonempty_string_field(name, "resource_id", context)?;
    optional_nonempty_string_field(name, "binding_kind", context)?;

    if namespace != expected_namespace {
        bail!("{context}.namespace must be {expected_namespace}");
    }
    if logical_name_id != format!("{expected_namespace}:{normalized_name}") {
        bail!(
            "{context}.logical_name_id {} does not match normalized_name {}",
            logical_name_id,
            normalized_name
        );
    }

    Ok(())
}

fn validate_verified_primary_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "{context} trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if trace.namespace != tuple.namespace {
        bail!(
            "{context} trace {} must use namespace {}",
            trace.execution_trace_id,
            tuple.namespace
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "{context} outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key = normalized_verified_primary_name_request_key(
        &tuple.namespace,
        &tuple.normalized_address,
        &tuple.coin_type,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "{context} trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        &format!("{context} trace.chain_context.requested_positions"),
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        &format!("{context} trace.chain_context.requested_positions"),
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        &format!("{context} trace.gateway_digests"),
    )?;
    if tuple.namespace == ENS_NAMESPACE && !gateway_digests.is_empty() {
        bail!("{context} must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        verified_primary_execution_source_family(&tuple.namespace)?,
        context,
    )? {
        bail!(
            "{context} must include source_family {} in manifest context or cache key",
            verified_primary_execution_source_family(&tuple.namespace)?
        );
    }

    let step_summary = if tuple.namespace == ENS_NAMESPACE {
        ensure_steps_do_not_use_deferred_execution_paths(
            &trace.steps,
            trace.execution_trace_id,
            context,
            SupportedResolutionPathClass::Direct,
        )?
    } else {
        ensure_steps_are_supported_basenames_verified_primary_path(
            trace,
            trace.execution_trace_id,
            matches!(
                verified_primary_name.status,
                VerifiedPrimaryNameStatus::Success
                    | VerifiedPrimaryNameStatus::Mismatch
                    | VerifiedPrimaryNameStatus::ExecutionFailed
            ),
        )?
    };
    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        if tuple.namespace == ENS_NAMESPACE && !step_summary.saw_universal_resolver_call {
            bail!(
                "{context} trace {} must include step_kind call_universal_resolver for status {:?}",
                trace.execution_trace_id,
                verified_primary_name.status
            );
        }
        match tuple.namespace.as_str() {
            ENS_NAMESPACE => ensure_contains_universal_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            BASENAMES_NAMESPACE => ensure_contains_basenames_l1_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            _ => unreachable!("unsupported verified-primary namespace already rejected"),
        }
    } else if !required_array(
        Some(&trace.contracts_called),
        &format!("{context} trace.contracts_called"),
    )?
    .is_empty()
    {
        match tuple.namespace.as_str() {
            ENS_NAMESPACE => ensure_contains_universal_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            BASENAMES_NAMESPACE => ensure_contains_basenames_l1_resolver_call(
                &trace.contracts_called,
                trace.execution_trace_id,
                context,
            )?,
            _ => unreachable!("unsupported verified-primary namespace already rejected"),
        }
    }

    validate_verified_primary_trace_terminal_payloads(trace, verified_primary_name)?;

    Ok(())
}

fn validate_verified_primary_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "{context} outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if outcome.namespace != tuple.namespace {
        bail!(
            "{context} outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            tuple.namespace
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "{context} outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "{context} trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "{context} outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    let expected_request_key = normalized_verified_primary_name_request_key(
        &tuple.namespace,
        &tuple.normalized_address,
        &tuple.coin_type,
    );
    if outcome.cache_key.request_key != expected_request_key {
        bail!(
            "{context} outcome request_key {} does not match expected {}",
            outcome.cache_key.request_key,
            expected_request_key
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "{context} outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        &format!("{context} cache_key.requested_chain_positions"),
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        &format!("{context} trace.chain_context.requested_positions"),
    )?;
    if trace_positions != requested_positions {
        bail!(
            "{context} trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            required_object(
                outcome.failure_payload.as_ref(),
                &format!("{context} execution_failed outcome.failure_payload"),
            )?;
        }
        _ if outcome.failure_payload.is_some() => {
            bail!(
                "{context} outcome for request_key {} must not set failure_payload unless status is execution_failed",
                outcome.cache_key.request_key
            );
        }
        _ => {}
    }

    Ok(())
}

fn validate_verified_primary_trace_terminal_payloads(
    trace: &ExecutionTrace,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&trace.namespace)?;
    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            if trace.final_payload.is_some() {
                bail!(
                    "{context} execution_failed trace {} must not set final_payload",
                    trace.execution_trace_id
                );
            }
            required_object(
                trace.failure_payload.as_ref(),
                &format!("{context} execution_failed trace.failure_payload"),
            )?;
        }
        _ => {
            if trace.failure_payload.is_some() {
                bail!(
                    "{context} trace {} must not set failure_payload unless status is execution_failed",
                    trace.execution_trace_id
                );
            }
            let final_payload = trace.final_payload.as_ref().with_context(|| {
                format!(
                    "{context} trace {} must set final_payload when status is not execution_failed",
                    trace.execution_trace_id
                )
            })?;
            let final_verified_primary_name = extract_verified_primary_name_section(
                Some(final_payload),
                &format!("{context} trace.final_payload"),
                &trace.namespace,
            )?;
            if final_verified_primary_name != *verified_primary_name {
                bail!(
                    "{context} trace.final_payload.verified_primary_name must match outcome_payload.verified_primary_name"
                );
            }
        }
    }

    Ok(())
}

async fn ensure_primary_name_anchor_exists(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if load_primary_name_current(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }

    bail!(
        "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
        tuple.normalized_address,
        tuple.namespace,
        tuple.coin_type
    )
}

fn extract_requested_selectors(trace: &ExecutionTrace) -> Result<RequestedSelectorSet> {
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    let surface = required_string(
        request_metadata,
        "surface",
        "ENS direct-path verified resolution trace.request_metadata",
    )?
    .to_owned();

    let ordered_record_keys = match (
        request_metadata.get("record_keys"),
        request_metadata.get("record_key"),
    ) {
        (Some(record_keys), Some(record_key)) => {
            let parsed_record_keys = parse_requested_record_keys(
                record_keys,
                "ENS direct-path verified resolution trace.request_metadata.record_keys",
            )?;
            let singular_record_key = record_key
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .context(
                    "ENS direct-path verified resolution trace.request_metadata must include non-empty string field record_key",
                )?;
            if parsed_record_keys.len() != 1 || parsed_record_keys[0] != singular_record_key {
                bail!(
                    "ENS direct-path verified resolution trace.request_metadata.record_key must match record_keys when both are present"
                );
            }
            parsed_record_keys
        }
        (Some(record_keys), None) => parse_requested_record_keys(
            record_keys,
            "ENS direct-path verified resolution trace.request_metadata.record_keys",
        )?,
        (None, Some(_)) => vec![
            required_string(
                request_metadata,
                "record_key",
                "ENS direct-path verified resolution trace.request_metadata",
            )?
            .to_owned(),
        ],
        (None, None) => bail!(
            "ENS direct-path verified resolution trace.request_metadata must include record_key or record_keys"
        ),
    };

    validate_ordered_record_keys(
        &ordered_record_keys,
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    let binding_kind = optional_nonempty_string_field(
        request_metadata,
        "binding_kind",
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    Ok(RequestedSelectorSet {
        surface,
        ordered_record_keys,
        binding_kind,
    })
}

fn parse_requested_record_keys(value: &Value, context: &str) -> Result<Vec<String>> {
    let items = required_array(Some(value), context)?;
    if items.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut record_keys = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        record_keys.push(
            item.as_str()
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("{context}[{index}] must be a non-empty string"))?
                .to_owned(),
        );
    }
    Ok(record_keys)
}

fn validate_ordered_record_keys(record_keys: &[String], context: &str) -> Result<()> {
    if record_keys.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut seen = BTreeSet::new();
    for record_key in record_keys {
        parse_supported_verified_record_key(record_key)?;
        if !seen.insert(record_key.clone()) {
            bail!("{context} must not contain duplicate selectors ({record_key})");
        }
    }

    Ok(())
}

fn extract_supported_verified_queries(
    outcome: &ExecutionOutcome,
) -> Result<Vec<VerifiedQuerySummary>> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("ENS direct-path verified resolution outcome must set outcome_payload")?;
    extract_verified_queries_from_payload(
        outcome_payload,
        "ENS direct-path verified resolution outcome_payload",
    )
}

fn extract_verified_queries_from_payload(
    payload: &Value,
    context: &str,
) -> Result<Vec<VerifiedQuerySummary>> {
    let payload = required_object(Some(payload), context)?;
    let verified_queries = required_array(
        payload.get("verified_queries"),
        &format!("{context}.verified_queries"),
    )?;
    if verified_queries.is_empty() {
        bail!("{context} must include at least one verified query");
    }

    let mut queries = Vec::with_capacity(verified_queries.len());
    let mut seen_record_keys = BTreeSet::new();
    for (index, query) in verified_queries.iter().enumerate() {
        let query_context = format!("{context}.verified_queries[{index}]");
        let query = required_object(Some(query), &query_context)?;
        if query.contains_key("unsupported_reason") {
            bail!("ENS direct-path verified resolution does not persist unsupported selectors");
        }

        let record_key = required_string(query, "record_key", &query_context)?.to_owned();
        if !seen_record_keys.insert(record_key.clone()) {
            bail!("{context}.verified_queries must not contain duplicate selectors ({record_key})");
        }

        let selector = parse_supported_verified_record_key(&record_key)?;
        let (status, value, failure_reason) = match required_string(
            query,
            "status",
            &query_context,
        )? {
            "success" => {
                let value = required_object(query.get("value"), &format!("{query_context}.value"))?;
                if let SupportedVerifiedRecordKey::Addr { coin_type } = &selector {
                    let value_coin_type =
                        required_string(value, "coin_type", &format!("{query_context}.value"))?;
                    if value_coin_type != coin_type {
                        bail!(
                            "ENS direct-path verified resolution query value coin_type {} does not match record_key {}",
                            value_coin_type,
                            record_key
                        );
                    }
                }
                let resolved_value = required_nonempty_string_field(
                    value,
                    "value",
                    &format!("{query_context}.value"),
                )?;
                if query.contains_key("failure_reason") {
                    bail!(
                        "ENS direct-path verified resolution success query must not set failure_reason"
                    );
                }
                (VerifiedQueryStatus::Success, Some(resolved_value), None)
            }
            "not_found" => {
                ensure_absent(query, "value", &query_context)?;
                let failure_reason =
                    optional_nonempty_string_field(query, "failure_reason", &query_context)?;
                (VerifiedQueryStatus::NotFound, None, failure_reason)
            }
            "execution_failed" => {
                ensure_absent(query, "value", &query_context)?;
                let failure_reason =
                    required_nonempty_string_field(query, "failure_reason", &query_context)?;
                (
                    VerifiedQueryStatus::ExecutionFailed,
                    None,
                    Some(failure_reason),
                )
            }
            status => bail!(
                "ENS direct-path verified resolution only supports success, not_found, and execution_failed selector results; found {status}"
            ),
        };

        queries.push(VerifiedQuerySummary {
            record_key,
            selector,
            status,
            value,
            failure_reason,
        });
    }

    Ok(queries)
}

fn ensure_requested_selectors_match_queries(
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if requested_selectors.ordered_record_keys.len() != queries.len() {
        bail!(
            "ENS direct-path verified resolution trace.request_metadata selectors {} do not match outcome verified query count {}",
            requested_selectors.ordered_record_keys.len(),
            queries.len()
        );
    }

    for (index, (requested_record_key, query)) in requested_selectors
        .ordered_record_keys
        .iter()
        .zip(queries.iter())
        .enumerate()
    {
        if requested_record_key != &query.record_key {
            bail!(
                "ENS direct-path verified resolution trace.request_metadata.record_keys[{index}] {} does not match outcome verified_queries[{index}] {}",
                requested_record_key,
                query.record_key
            );
        }
    }

    Ok(())
}

fn validate_trace(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if trace.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution trace {} must use namespace {}",
            trace.execution_trace_id,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key = normalized_request_key(
        ENS_NAMESPACE,
        &requested_selectors.surface,
        &requested_selectors.ordered_record_keys,
    );
    if trace.request_key != expected_request_key {
        bail!(
            "ENS direct-path verified resolution trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "ENS direct-path verified resolution trace.gateway_digests",
    )?;
    if !gateway_digests.is_empty() {
        bail!("ENS direct-path verified resolution must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        ENS_EXECUTION_SOURCE_FAMILY,
        "ENS direct-path verified resolution",
    )? {
        bail!(
            "ENS direct-path verified resolution must include source_family {} in manifest context or cache key",
            ENS_EXECUTION_SOURCE_FAMILY
        );
    }

    ensure_contains_universal_resolver_call(
        &trace.contracts_called,
        trace.execution_trace_id,
        "ENS direct-path verified resolution",
    )?;
    ensure_steps_are_supported_exact_surface_path(
        trace,
        requested_selectors,
        trace.execution_trace_id,
    )?;
    validate_trace_terminal_payloads(trace, queries)?;

    Ok(())
}

fn validate_outcome(
    outcome: &ExecutionOutcome,
    trace: &ExecutionTrace,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_RESOLUTION_REQUEST_TYPE
        );
    }
    if outcome.namespace != ENS_NAMESPACE {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS direct-path verified resolution outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "ENS direct-path verified resolution outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "ENS direct-path verified resolution outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS direct-path verified resolution trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "ENS direct-path verified resolution trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    if queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed)
    {
        required_object(
            outcome.failure_payload.as_ref(),
            "ENS direct-path verified resolution execution_failed outcome.failure_payload",
        )?;
    } else if outcome.failure_payload.is_some() {
        bail!(
            "ENS direct-path verified resolution outcome for request_key {} must not set failure_payload unless every selector status is execution_failed",
            outcome.cache_key.request_key
        );
    }

    Ok(())
}

fn validate_trace_terminal_payloads(
    trace: &ExecutionTrace,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    let all_execution_failed = queries
        .iter()
        .all(|query| query.status == VerifiedQueryStatus::ExecutionFailed);

    if all_execution_failed {
        if trace.final_payload.is_some() {
            bail!(
                "ENS direct-path verified resolution execution_failed trace {} must not set final_payload",
                trace.execution_trace_id
            );
        }
        required_object(
            trace.failure_payload.as_ref(),
            "ENS direct-path verified resolution execution_failed trace.failure_payload",
        )?;
        return Ok(());
    }

    if trace.failure_payload.is_some() {
        bail!(
            "ENS direct-path verified resolution trace {} must not set failure_payload unless every selector status is execution_failed",
            trace.execution_trace_id
        );
    }

    let final_payload = trace.final_payload.as_ref().with_context(|| {
        format!(
            "ENS direct-path verified resolution trace {} must set final_payload when any selector resolves or returns not_found",
            trace.execution_trace_id
        )
    })?;
    if final_payload_contains_verified_queries(final_payload)? {
        let final_queries = extract_verified_queries_from_payload(
            final_payload,
            "ENS direct-path verified resolution trace.final_payload",
        )?;
        if final_queries != queries {
            bail!(
                "ENS direct-path verified resolution trace.final_payload.verified_queries must match outcome_payload.verified_queries"
            );
        }
        return Ok(());
    }

    if queries.len() != 1 {
        bail!(
            "ENS direct-path verified resolution multi-selector trace {} final_payload must include verified_queries",
            trace.execution_trace_id
        );
    }

    match queries[0].status {
        VerifiedQueryStatus::Success => validate_success_final_payload(final_payload, &queries[0]),
        VerifiedQueryStatus::NotFound => {
            validate_not_found_final_payload(final_payload, &queries[0])
        }
        VerifiedQueryStatus::ExecutionFailed => unreachable!("all execution_failed handled above"),
    }
}

fn validate_raw_call_snapshots(
    raw_call_snapshots: &[RawCallSnapshot],
    outcome: &ExecutionOutcome,
    requested_selectors: &RequestedSelectorSet,
) -> Result<()> {
    if raw_call_snapshots.is_empty() {
        return Ok(());
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS direct-path verified resolution cache_key.requested_chain_positions",
    )?;
    let requested_position = requested_positions
        .first()
        .context("ENS direct-path verified resolution must include one requested chain position")?;

    for snapshot in raw_call_snapshots {
        if snapshot.chain_id != requested_position.chain_id
            || snapshot.block_hash != requested_position.block_hash
            || snapshot.block_number != requested_position.block_number
        {
            bail!(
                "ENS direct-path verified resolution raw call snapshot for request {} must align with requested chain position {} {} {}",
                normalized_request_key(
                    ENS_NAMESPACE,
                    &requested_selectors.surface,
                    &requested_selectors.ordered_record_keys,
                ),
                requested_position.chain_id,
                requested_position.block_number,
                requested_position.block_hash
            );
        }
    }

    Ok(())
}

fn parse_supported_verified_record_key(record_key: &str) -> Result<SupportedVerifiedRecordKey> {
    parse_supported_verified_resolution_record_key(record_key)
}

fn validate_success_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    let record_kind = required_string(
        object,
        "record_kind",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    match &query.selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => {
            if record_kind != "addr" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be addr, found {}",
                    record_kind
                );
            }
            let payload_coin_type = required_coin_type_field(
                object,
                "coin_type",
                "ENS direct-path verified resolution success trace.final_payload",
            )?;
            if &payload_coin_type != coin_type {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.coin_type {} does not match outcome record_key {}",
                    payload_coin_type,
                    query.record_key
                );
            }
        }
        SupportedVerifiedRecordKey::Contenthash => {
            if record_kind != "contenthash" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be contenthash, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Avatar => {
            if record_kind != "avatar" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be avatar, found {}",
                    record_kind
                );
            }
        }
        SupportedVerifiedRecordKey::Text => {
            if record_kind != "text" {
                bail!(
                    "ENS direct-path verified resolution success trace.final_payload.record_kind must be text, found {}",
                    record_kind
                );
            }
        }
    }
    let value = required_nonempty_string_field(
        object,
        "value",
        "ENS direct-path verified resolution success trace.final_payload",
    )?;
    if query
        .value
        .as_deref()
        .is_some_and(|expected_value| expected_value != value)
    {
        bail!(
            "ENS direct-path verified resolution success trace.final_payload.value {} does not match outcome query value {}",
            value,
            query.value.as_deref().unwrap_or_default()
        );
    }
    Ok(())
}

fn validate_not_found_final_payload(
    final_payload: &Value,
    query: &VerifiedQuerySummary,
) -> Result<()> {
    let final_payload_object = required_object(
        Some(final_payload),
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    let failure_reason = optional_nonempty_string_field(
        final_payload_object,
        "failure_reason",
        "ENS direct-path verified resolution not_found trace.final_payload",
    )?;
    if failure_reason != query.failure_reason {
        bail!(
            "ENS direct-path verified resolution not_found trace.final_payload.failure_reason {:?} does not match outcome query failure_reason {:?}",
            failure_reason,
            query.failure_reason
        );
    }
    Ok(())
}

fn final_payload_contains_verified_queries(final_payload: &Value) -> Result<bool> {
    Ok(required_object(
        Some(final_payload),
        "ENS direct-path verified resolution trace.final_payload",
    )?
    .contains_key("verified_queries"))
}

fn normalized_request_key(
    namespace: &str,
    surface: &str,
    ordered_record_keys: &[String],
) -> String {
    bigname_storage::normalized_resolution_request_key_from_record_keys(
        namespace,
        surface,
        ordered_record_keys,
    )
}

fn normalized_verified_primary_name_request_key(
    namespace: &str,
    normalized_address: &str,
    coin_type: &str,
) -> String {
    format!(
        "{namespace}:{}:{coin_type}",
        normalize_address(normalized_address)
    )
}

fn ensure_steps_are_supported_basenames_verified_primary_path(
    trace: &ExecutionTrace,
    execution_trace_id: Uuid,
    require_l1_resolver_step: bool,
) -> Result<SupportedResolutionStepSummary> {
    let mut saw_l1_resolver_call = false;
    for step in &trace.steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("alias")
            || normalized.contains("wildcard")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("universal_resolver")
        {
            bail!(
                "Basenames verified-primary trace {} must not persist out-of-class step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("l1_resolver") {
            saw_l1_resolver_call = true;
        }
    }

    if require_l1_resolver_step && !saw_l1_resolver_call {
        bail!(
            "Basenames verified-primary trace {} must include an L1 resolver step",
            execution_trace_id
        );
    }

    Ok(SupportedResolutionStepSummary::default())
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

fn manifest_versions_include_source_family_for_context(
    manifest_context: Option<&Value>,
    cache_manifest_versions: Option<&Value>,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    if let Some(manifest_context) = manifest_context {
        let object = required_object(
            Some(manifest_context),
            &format!("{context} trace.manifest_context"),
        )?;
        if contains_source_family(
            object.get("manifest_versions"),
            expected_source_family,
            context,
        )? {
            return Ok(true);
        }
    }

    contains_source_family(cache_manifest_versions, expected_source_family, context)
}

fn contains_source_family(
    value: Option<&Value>,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let items = required_array(Some(value), &format!("{context} manifest_versions"))?;
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context} manifest_versions[{index}]"))?;
        if object
            .get("source_family")
            .and_then(Value::as_str)
            .is_some_and(|value| value == expected_source_family)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_contains_universal_resolver_call(
    contracts_called: &Value,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    let calls = required_array(
        Some(contracts_called),
        &format!("{context} trace.contracts_called"),
    )?;
    for (index, call) in calls.iter().enumerate() {
        let object = required_object(
            Some(call),
            &format!("{context} trace.contracts_called[{index}]"),
        )?;
        let chain_id = required_string(
            object,
            "chain_id",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let contract_address = required_string(
            object,
            "contract_address",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let selector = required_string(
            object,
            "selector",
            &format!("{context} trace.contracts_called entry"),
        )?;
        if chain_id == ETHEREUM_MAINNET_CHAIN_ID
            && contract_address.eq_ignore_ascii_case(ENS_UNIVERSAL_RESOLVER_ADDRESS)
            && !selector.is_empty()
        {
            return Ok(());
        }
    }

    bail!(
        "{context} trace {} must include one {} contract call on {}",
        execution_trace_id,
        ENS_UNIVERSAL_RESOLVER_ROLE,
        ETHEREUM_MAINNET_CHAIN_ID
    )
}

fn ensure_contains_basenames_l1_resolver_call(
    contracts_called: &Value,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    let calls = required_array(
        Some(contracts_called),
        &format!("{context} trace.contracts_called"),
    )?;
    for (index, call) in calls.iter().enumerate() {
        let object = required_object(
            Some(call),
            &format!("{context} trace.contracts_called[{index}]"),
        )?;
        let chain_id = required_string(
            object,
            "chain_id",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let contract_address = required_string(
            object,
            "contract_address",
            &format!("{context} trace.contracts_called entry"),
        )?;
        let selector = required_string(
            object,
            "selector",
            &format!("{context} trace.contracts_called entry"),
        )?;
        if chain_id == ETHEREUM_MAINNET_CHAIN_ID
            && contract_address.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
            && !selector.is_empty()
        {
            return Ok(());
        }
    }

    bail!(
        "{context} trace {} must include one {} contract call on {}",
        execution_trace_id,
        BASENAMES_L1_RESOLVER_ROLE,
        ETHEREUM_MAINNET_CHAIN_ID
    )
}

fn ensure_steps_are_supported_exact_surface_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    let path_class = classify_supported_resolution_path(
        requested_selectors.binding_kind.as_deref(),
        execution_trace_id,
    )?;
    let step_summary = ensure_steps_do_not_use_deferred_execution_paths(
        &trace.steps,
        execution_trace_id,
        "ENS direct-path verified resolution",
        path_class,
    )?;
    if !step_summary.saw_universal_resolver_call {
        bail!(
            "ENS direct-path verified resolution trace {} must include step_kind call_universal_resolver",
            execution_trace_id
        );
    }
    ensure_universal_resolver_steps_anchor_to_surface(
        &trace.steps,
        &requested_selectors.surface,
        execution_trace_id,
        "ENS direct-path verified resolution",
    )?;
    validate_supported_exact_surface_runtime_details(trace, path_class, execution_trace_id)?;
    match path_class {
        SupportedResolutionPathClass::Direct => {
            if step_summary.saw_alias_step {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias steps without binding_kind {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::AliasOnly => {}
        SupportedResolutionPathClass::WildcardDerived => {
            if step_summary.saw_alias_step {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias steps when binding_kind is {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

fn ensure_steps_are_supported_basenames_transport_direct_path(
    trace: &ExecutionTrace,
    requested_selectors: &RequestedSelectorSet,
    execution_trace_id: Uuid,
) -> Result<()> {
    match requested_selectors.binding_kind.as_deref() {
        None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {}
        Some(other) => bail!(
            "Basenames transport-direct verified resolution trace {} must use binding_kind {} or omit binding_kind; found {}",
            execution_trace_id,
            DECLARED_REGISTRY_PATH_BINDING_KIND,
            other
        ),
    }

    let mut saw_l1_resolver_call = false;
    let mut saw_ccip_or_proof = false;
    for step in &trace.steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("alias")
            || normalized.contains("wildcard")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("universal_resolver")
        {
            bail!(
                "Basenames transport-direct verified resolution trace {} must not persist out-of-class step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("l1_resolver") {
            saw_l1_resolver_call = true;
            let payload = required_object(
                Some(&step.step_payload),
                "Basenames transport-direct verified resolution trace.steps.l1_resolver.step_payload",
            )?;
            if let Some(name) = payload.get("name").and_then(Value::as_str)
                && name != requested_selectors.surface
            {
                bail!(
                    "Basenames transport-direct verified resolution trace {} must anchor L1 resolver name {} to request surface {}",
                    execution_trace_id,
                    name,
                    requested_selectors.surface
                );
            }
        }
        if normalized.contains("ccip")
            || normalized.contains("offchain")
            || normalized.contains("resolve_with_proof")
            || normalized.contains("proof")
        {
            saw_ccip_or_proof = true;
        }
    }

    if !saw_l1_resolver_call {
        bail!(
            "Basenames transport-direct verified resolution trace {} must include an L1 resolver step",
            execution_trace_id
        );
    }
    if !saw_ccip_or_proof {
        bail!(
            "Basenames transport-direct verified resolution trace {} must include CCIP or proof-completion steps",
            execution_trace_id
        );
    }

    ensure_basenames_alias_detail_absent(trace, "Basenames transport-direct verified resolution")?;
    ensure_basenames_wildcard_detail_absent(
        trace,
        "Basenames transport-direct verified resolution",
    )?;
    ensure_basenames_transport_detail_supported(
        trace,
        "Basenames transport-direct verified resolution",
    )?;

    Ok(())
}

fn ensure_steps_do_not_use_deferred_execution_paths(
    steps: &[bigname_storage::ExecutionTraceStep],
    execution_trace_id: Uuid,
    context: &str,
    path_class: SupportedResolutionPathClass,
) -> Result<SupportedResolutionStepSummary> {
    let mut summary = SupportedResolutionStepSummary::default();
    for step in steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("wildcard")
            && path_class != SupportedResolutionPathClass::WildcardDerived
        {
            bail!(
                "{context} trace {} must not persist wildcard traversal step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("ccip")
            || normalized.contains("transport")
            || normalized.contains("subregistry")
            || normalized.contains("ancestor")
            || normalized.contains("basename")
        {
            bail!(
                "{context} trace {} must not persist non-direct step {}",
                execution_trace_id,
                step.step_kind
            );
        }
        if normalized.contains("alias") {
            summary.saw_alias_step = true;
        }
        if step.step_kind == "call_universal_resolver" {
            summary.saw_universal_resolver_call = true;
        }
    }

    Ok(summary)
}

fn classify_supported_resolution_path(
    binding_kind: Option<&str>,
    execution_trace_id: Uuid,
) -> Result<SupportedResolutionPathClass> {
    match binding_kind {
        None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {
            Ok(SupportedResolutionPathClass::Direct)
        }
        Some(RESOLVER_ALIAS_PATH_BINDING_KIND) => Ok(SupportedResolutionPathClass::AliasOnly),
        Some(OBSERVED_WILDCARD_PATH_BINDING_KIND) => {
            Ok(SupportedResolutionPathClass::WildcardDerived)
        }
        Some(LINKED_SUBREGISTRY_PATH_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist non-alias ancestor-selected binding_kind {}",
            execution_trace_id,
            LINKED_SUBREGISTRY_PATH_BINDING_KIND
        ),
        Some(MIGRATION_REBIND_BINDING_KIND | OBSERVED_ONLY_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist unsupported binding_kind {}",
            execution_trace_id,
            binding_kind.unwrap_or_default()
        ),
        Some(other) => bail!(
            "ENS direct-path verified resolution trace {} must use binding_kind {}, {}, or omit binding_kind; found {}",
            execution_trace_id,
            DECLARED_REGISTRY_PATH_BINDING_KIND,
            RESOLVER_ALIAS_PATH_BINDING_KIND,
            other
        ),
    }
}

fn validate_supported_exact_surface_runtime_details(
    trace: &ExecutionTrace,
    path_class: SupportedResolutionPathClass,
    execution_trace_id: Uuid,
) -> Result<()> {
    let alias_present =
        persisted_alias_detail_is_present(trace, "ENS direct-path verified resolution")?;
    ensure_wildcard_detail_matches_path_class(
        trace,
        path_class,
        "ENS direct-path verified resolution",
        execution_trace_id,
    )?;
    ensure_transport_detail_absent(trace, "ENS direct-path verified resolution")?;

    match path_class {
        SupportedResolutionPathClass::Direct => {
            if alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias detail unless binding_kind is {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::AliasOnly => {
            if !alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must persist alias.final_target and non-empty alias.hops for binding_kind {}",
                    execution_trace_id,
                    RESOLVER_ALIAS_PATH_BINDING_KIND
                );
            }
        }
        SupportedResolutionPathClass::WildcardDerived => {
            if alias_present {
                bail!(
                    "ENS direct-path verified resolution trace {} must not persist alias detail when binding_kind is {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

fn ensure_universal_resolver_steps_anchor_to_surface(
    steps: &[bigname_storage::ExecutionTraceStep],
    surface: &str,
    execution_trace_id: Uuid,
    context: &str,
) -> Result<()> {
    for step in steps {
        if step.step_kind != "call_universal_resolver" {
            continue;
        }

        let payload = required_object(
            Some(&step.step_payload),
            &format!("{context} trace.steps.call_universal_resolver.step_payload"),
        )?;
        if let Some(name) = payload.get("name").and_then(Value::as_str)
            && name != surface
        {
            bail!(
                "{context} trace {} must anchor call_universal_resolver name {} to request surface {}",
                execution_trace_id,
                name,
                surface
            );
        }
    }

    Ok(())
}

fn persisted_alias_detail_is_present(trace: &ExecutionTrace, context: &str) -> Result<bool> {
    let Some(alias) = persisted_trace_detail_object(trace, "alias") else {
        return Ok(false);
    };

    let alias_context = format!("{context} trace alias detail");
    let alias = required_object(Some(&alias), &alias_context)?;
    ensure_only_allowed_fields(alias, &["final_target", "hops"], &alias_context)?;

    let final_target = match alias.get("final_target") {
        None | Some(Value::Null) => None,
        Some(value) => {
            validate_verified_primary_name_ref(
                Some(value),
                &format!("{alias_context}.final_target"),
                &trace.namespace,
            )?;
            Some(value)
        }
    };
    let hops = required_array(alias.get("hops"), &format!("{alias_context}.hops"))?;

    if final_target.is_none() && hops.is_empty() {
        return Ok(false);
    }
    if final_target.is_none() || hops.is_empty() {
        bail!("{alias_context} must set final_target and non-empty hops together");
    }

    for (index, hop) in hops.iter().enumerate() {
        validate_verified_primary_name_ref(
            Some(hop),
            &format!("{alias_context}.hops[{index}]"),
            &trace.namespace,
        )?;
    }
    if hops.last() != final_target {
        bail!("{alias_context}.hops last element must match final_target");
    }

    Ok(true)
}

fn ensure_wildcard_detail_matches_path_class(
    trace: &ExecutionTrace,
    path_class: SupportedResolutionPathClass,
    context: &str,
    execution_trace_id: Uuid,
) -> Result<()> {
    let Some(wildcard) = persisted_trace_detail_object(trace, "wildcard") else {
        if path_class == SupportedResolutionPathClass::WildcardDerived {
            bail!(
                "{context} trace {} must persist wildcard.source non-null with matched_labels non-empty for binding_kind {}",
                execution_trace_id,
                OBSERVED_WILDCARD_PATH_BINDING_KIND
            );
        }
        return Ok(());
    };

    let wildcard_context = format!("{context} trace wildcard detail");
    let wildcard = required_object(Some(&wildcard), &wildcard_context)?;
    ensure_only_allowed_fields(wildcard, &["source", "matched_labels"], &wildcard_context)?;

    let source_present = match wildcard.get("source") {
        None | Some(Value::Null) => false,
        Some(source) => {
            validate_verified_primary_name_ref(
                Some(source),
                &format!("{wildcard_context}.source"),
                &trace.namespace,
            )?;
            true
        }
    };
    let matched_labels = required_array(
        wildcard.get("matched_labels"),
        &format!("{wildcard_context}.matched_labels"),
    )?;
    match path_class {
        SupportedResolutionPathClass::Direct | SupportedResolutionPathClass::AliasOnly => {
            if source_present || !matched_labels.is_empty() {
                bail!(
                    "{context} only supports wildcard.source=null with matched_labels=[] for persisted exact-surface requests"
                );
            }
        }
        SupportedResolutionPathClass::WildcardDerived => {
            if !source_present || matched_labels.is_empty() {
                bail!(
                    "{context} trace {} must persist wildcard.source non-null with matched_labels non-empty for binding_kind {}",
                    execution_trace_id,
                    OBSERVED_WILDCARD_PATH_BINDING_KIND
                );
            }
        }
    }

    Ok(())
}

fn ensure_transport_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(transport) = persisted_trace_detail_object(trace, "transport") else {
        return Ok(());
    };

    let transport_context = format!("{context} trace transport detail");
    let transport = required_object(Some(&transport), &transport_context)?;
    ensure_only_allowed_fields(
        transport,
        &[
            "source_chain_id",
            "target_chain_id",
            "contract_address",
            "latest_event_kind",
        ],
        &transport_context,
    )?;

    for field_name in [
        "source_chain_id",
        "target_chain_id",
        "contract_address",
        "latest_event_kind",
    ] {
        if !matches!(transport.get(field_name), None | Some(Value::Null)) {
            bail!("{context} transport-assisted persisted requests remain unsupported");
        }
    }

    Ok(())
}

fn ensure_basenames_alias_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(alias) = persisted_trace_detail_object(trace, "alias") else {
        return Ok(());
    };
    let alias = required_object(Some(&alias), &format!("{context} trace alias detail"))?;
    let final_target_present = !matches!(alias.get("final_target"), None | Some(Value::Null));
    let hops = required_array(
        alias.get("hops"),
        &format!("{context} trace alias detail.hops"),
    )?;
    if final_target_present || !hops.is_empty() {
        bail!("{context} must keep alias.final_target null with alias.hops empty");
    }
    Ok(())
}

fn ensure_basenames_wildcard_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(wildcard) = persisted_trace_detail_object(trace, "wildcard") else {
        return Ok(());
    };
    let wildcard = required_object(Some(&wildcard), &format!("{context} trace wildcard detail"))?;
    let source_present = !matches!(wildcard.get("source"), None | Some(Value::Null));
    let matched_labels = required_array(
        wildcard.get("matched_labels"),
        &format!("{context} trace wildcard detail.matched_labels"),
    )?;
    if source_present || !matched_labels.is_empty() {
        bail!("{context} must keep wildcard.source null with matched_labels empty");
    }
    Ok(())
}

fn ensure_basenames_transport_detail_supported(
    trace: &ExecutionTrace,
    context: &str,
) -> Result<()> {
    let transport = persisted_trace_detail_object(trace, "transport")
        .context(format!("{context} must persist transport detail"))?;
    let transport = required_object(
        Some(&transport),
        &format!("{context} trace transport detail"),
    )?;
    ensure_only_allowed_fields(
        transport,
        &[
            "source_chain_id",
            "target_chain_id",
            "contract_address",
            "latest_event_kind",
        ],
        &format!("{context} trace transport detail"),
    )?;

    let source_chain_id = required_string(
        transport,
        "source_chain_id",
        &format!("{context} trace transport detail"),
    )?;
    let target_chain_id = required_string(
        transport,
        "target_chain_id",
        &format!("{context} trace transport detail"),
    )?;
    let contract_address = required_string(
        transport,
        "contract_address",
        &format!("{context} trace transport detail"),
    )?;

    if source_chain_id != BASE_MAINNET_CHAIN_ID
        || target_chain_id != ETHEREUM_MAINNET_CHAIN_ID
        || !contract_address.eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
    {
        bail!(
            "{context} must use transport {} -> {} via {}",
            BASE_MAINNET_CHAIN_ID,
            ETHEREUM_MAINNET_CHAIN_ID,
            BASENAMES_L1_RESOLVER_ADDRESS
        );
    }

    Ok(())
}

fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<Value> {
    trace
        .request_metadata
        .get(key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace.steps.iter().find_map(|step| {
                step.step_payload
                    .get(key)
                    .filter(|value| value.is_object())
                    .cloned()
            })
        })
}

fn ensure_single_ethereum_mainnet_position(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 1 {
        bail!(
            "{context} must include exactly one chain position, found {}",
            positions.len()
        );
    }
    let position = &positions[0];
    if position.chain_id != ETHEREUM_MAINNET_CHAIN_ID {
        bail!(
            "{context} must target chain_id {}, found {}",
            ETHEREUM_MAINNET_CHAIN_ID,
            position.chain_id
        );
    }
    Ok(())
}

fn ensure_basenames_requested_positions(
    positions: &[RequestedChainPosition],
    context: &str,
) -> Result<()> {
    if positions.len() != 2 {
        bail!(
            "{context} must include exactly two chain positions for {} -> {}, found {}",
            BASE_MAINNET_CHAIN_ID,
            ETHEREUM_MAINNET_CHAIN_ID,
            positions.len()
        );
    }

    let mut saw_base = false;
    let mut saw_ethereum = false;
    for position in positions {
        match position.chain_id.as_str() {
            BASE_MAINNET_CHAIN_ID => saw_base = true,
            ETHEREUM_MAINNET_CHAIN_ID => saw_ethereum = true,
            other => {
                bail!(
                    "{context} only supports chain_id {} and {}, found {}",
                    BASE_MAINNET_CHAIN_ID,
                    ETHEREUM_MAINNET_CHAIN_ID,
                    other
                )
            }
        }
    }

    if !saw_base {
        bail!("{context} must include chain_id {}", BASE_MAINNET_CHAIN_ID);
    }
    if !saw_ethereum {
        bail!(
            "{context} must include chain_id {}",
            ETHEREUM_MAINNET_CHAIN_ID
        );
    }
    Ok(())
}

fn required_chain_positions(
    value: Option<&Value>,
    context: &str,
) -> Result<Vec<RequestedChainPosition>> {
    let items = required_array(value, context)?;
    let mut positions = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = required_object(Some(item), &format!("{context}[{index}]"))?;
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .with_context(|| {
                format!("{context}[{index}] must include integer field block_number")
            })?;
        positions.push(RequestedChainPosition {
            chain_id: required_string(object, "chain_id", &format!("{context}[{index}]"))?
                .to_owned(),
            block_number,
            block_hash: required_string(object, "block_hash", &format!("{context}[{index}]"))?
                .to_owned(),
        });
    }
    Ok(positions)
}

fn required_object<'a>(value: Option<&'a Value>, context: &str) -> Result<&'a Map<String, Value>> {
    value
        .and_then(Value::as_object)
        .with_context(|| format!("{context} must be a JSON object"))
}

fn required_array<'a>(value: Option<&'a Value>, context: &str) -> Result<&'a Vec<Value>> {
    value
        .and_then(Value::as_array)
        .with_context(|| format!("{context} must be a JSON array"))
}

fn ensure_only_allowed_fields(
    object: &Map<String, Value>,
    allowed_fields: &[&str],
    context: &str,
) -> Result<()> {
    for key in object.keys() {
        if !allowed_fields
            .iter()
            .any(|allowed| allowed == &key.as_str())
        {
            bail!("{context} must not set field {key}");
        }
    }

    Ok(())
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

fn required_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    Ok(required_string(object, field_name, context)?.to_owned())
}

fn optional_nonempty_string_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<Option<String>> {
    match object.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => bail!("{context} field {field_name} must be null or a non-empty string"),
    }
}

fn required_coin_type_field(
    object: &Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<String> {
    match object.get(field_name) {
        Some(Value::String(value))
            if !value.is_empty() && value.as_bytes().iter().all(u8::is_ascii_digit) =>
        {
            Ok(value.clone())
        }
        Some(Value::Number(value)) if value.as_u64().is_some_and(|coin_type| coin_type > 0) => {
            Ok(value.to_string())
        }
        _ => bail!("{context} field {field_name} must be decimal coin_type text or number"),
    }
}

fn ensure_absent(object: &Map<String, Value>, field_name: &str, context: &str) -> Result<()> {
    if object.contains_key(field_name) {
        bail!("{context} must not set field {field_name}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        process::Command,
        str::FromStr,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use serde_json::json;
    use sqlx::{
        ConnectOptions, PgPool, Row,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };
    use tokio::time::{Duration, timeout};

    use super::*;
    use bigname_storage::{MIGRATOR, NormalizedEvent, default_database_url};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
    static WORKER_CARGO_LOCK: Mutex<()> = Mutex::new(());

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            Self::new_with_max_connections(5).await
        }

        async fn new_with_max_connections(max_connections: u32) -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for execution tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_execution_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for execution tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(max_connections)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect execution test pool")?;

            MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for execution tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn raw_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> bigname_storage::RawBlock {
        bigname_storage::RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(block_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }
    }

    fn chain_lineage_block(
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
        block_timestamp: i64,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: Some(format!("0xparent{block_number:08x}")),
            block_number,
            block_timestamp: timestamp(block_timestamp),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn logical_name_id_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> String {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        format!(
            "{}:{}",
            request.trace.namespace, requested_selectors.surface
        )
    }

    async fn rebuild_name_current_projection(
        database: &TestDatabase,
        logical_name_id: &str,
    ) -> Result<()> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for execution name_current rebuild")?;
        let rebuild_database_url = base_options
            .database(&database.database_name)
            .to_url_lossy()
            .to_string();
        let logical_name_id = logical_name_id.to_owned();
        let worker_manifest_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = WORKER_CARGO_LOCK
                .lock()
                .expect("worker cargo lock must not be poisoned");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let output = Command::new(cargo)
                .arg("run")
                .arg("--quiet")
                .arg("--manifest-path")
                .arg(worker_manifest_path)
                .arg("--")
                .arg("name-current")
                .arg("rebuild")
                .arg("--database-url")
                .arg(&rebuild_database_url)
                .arg("--logical-name-id")
                .arg(&logical_name_id)
                .output()
                .with_context(|| {
                    format!(
                        "failed to invoke worker name_current rebuild for {logical_name_id}"
                    )
                })?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "worker name_current rebuild failed for {logical_name_id}\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }

            Ok(())
        })
        .await
        .context("execution name_current rebuild task panicked")??;

        Ok(())
    }

    fn requested_chain_positions_json_from_projection(chain_positions: &Value) -> Result<Value> {
        Ok(Value::Array(
            build_requested_chain_positions_from_projection(chain_positions)?
                .into_iter()
                .map(|position| {
                    json!({
                        "chain_id": position.chain_id,
                        "block_number": position.block_number,
                        "block_hash": position.block_hash,
                    })
                })
                .collect(),
        ))
    }

    fn append_cache_key_part(buffer: &mut String, value: &str) {
        use std::fmt::Write as _;

        write!(buffer, "{}:{value};", value.len())
            .expect("string write to cache-key buffer must succeed");
    }

    fn manifest_versions_for_request_from_projection(value: &Value) -> Result<Value> {
        let items = value
            .as_array()
            .context("rebuilt execution test manifest_versions must be an array")?;
        let mut sanitized = items
            .iter()
            .enumerate()
            .map(|(index, item)| -> Result<(String, Value)> {
                let object = item.as_object().with_context(|| {
                    format!("rebuilt execution test manifest_versions[{index}] must be an object")
                })?;
                let mut manifest_version = Map::new();
                let mut identity_key = String::new();
                let source_manifest_id = object
                    .get("source_manifest_id")
                    .and_then(Value::as_i64)
                    .filter(|value| *value > 0);
                append_cache_key_part(
                    &mut identity_key,
                    &source_manifest_id
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                );
                if let Some(source_manifest_id) = source_manifest_id {
                    manifest_version.insert(
                        "source_manifest_id".to_owned(),
                        Value::Number(source_manifest_id.into()),
                    );
                }
                let source_family = object
                    .get("source_family")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                append_cache_key_part(
                    &mut identity_key,
                    source_family.as_deref().unwrap_or_default(),
                );
                if let Some(source_family) = source_family {
                    manifest_version.insert(
                        "source_family".to_owned(),
                        Value::String(source_family),
                    );
                }
                let manifest_version_number = object
                    .get("manifest_version")
                    .and_then(Value::as_i64)
                    .filter(|value| *value > 0)
                    .with_context(|| {
                        format!(
                            "rebuilt execution test manifest_versions[{index}].manifest_version must be positive"
                        )
                    })?;
                append_cache_key_part(&mut identity_key, &manifest_version_number.to_string());
                manifest_version.insert(
                    "manifest_version".to_owned(),
                    Value::Number(manifest_version_number.into()),
                );
                Ok((identity_key, Value::Object(manifest_version)))
            })
            .collect::<Result<Vec<_>>>()?;
        sanitized.sort_by(|left, right| left.0.cmp(&right.0));
        sanitized.dedup_by(|left, right| left.0 == right.0);
        Ok(Value::Array(
            sanitized
                .into_iter()
                .map(|(_, manifest_version)| manifest_version)
                .collect(),
        ))
    }

    fn sync_request_with_rebuilt_name_current(
        request: &mut PersistEnsExactNameVerifiedResolutionRequest,
        row: &NameCurrentRow,
    ) -> Result<()> {
        let topology = row
            .declared_summary
            .get("topology")
            .cloned()
            .context("rebuilt execution test row must project topology")?;
        let version_boundaries = topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .context("rebuilt execution test topology must include version_boundaries")?;
        request.outcome.cache_key.manifest_versions =
            manifest_versions_for_request_from_projection(
                row.provenance
                    .get("manifest_versions")
                    .unwrap_or(&Value::Null),
            )?;
        request.outcome.cache_key.requested_chain_positions =
            requested_chain_positions_json_from_projection(&row.chain_positions)?;
        request.outcome.cache_key.topology_version_boundary = version_boundaries
            .get("topology_version_boundary")
            .cloned()
            .context("rebuilt execution test topology must include topology boundary")?;
        request.outcome.cache_key.record_version_boundary = version_boundaries
            .get("record_version_boundary")
            .cloned()
            .context("rebuilt execution test topology must include record boundary")?;
        request.trace.chain_context["requested_positions"] =
            request.outcome.cache_key.requested_chain_positions.clone();
        let requested_positions = request
            .outcome
            .cache_key
            .requested_chain_positions
            .as_array()
            .cloned()
            .unwrap_or_default();
        for snapshot in &mut request.raw_call_snapshots {
            let requested_position = requested_positions
                .iter()
                .find(|position| {
                    position
                        .get("chain_id")
                        .and_then(Value::as_str)
                        .is_some_and(|chain_id| chain_id == snapshot.chain_id)
                })
                .or_else(|| requested_positions.first());
            if let Some(requested_position) = requested_position {
                if let Some(chain_id) = requested_position.get("chain_id").and_then(Value::as_str) {
                    snapshot.chain_id = chain_id.to_owned();
                }
                if let Some(block_number) = requested_position
                    .get("block_number")
                    .and_then(Value::as_i64)
                {
                    snapshot.block_number = block_number;
                }
                if let Some(block_hash) =
                    requested_position.get("block_hash").and_then(Value::as_str)
                {
                    snapshot.block_hash = block_hash.to_owned();
                }
            }
        }

        if request.trace.namespace == ENS_NAMESPACE {
            request.trace.request_metadata["alias"] = topology["alias"].clone();
            request.trace.request_metadata["wildcard"] = topology["wildcard"].clone();
        }
        if request.trace.namespace == BASENAMES_NAMESPACE {
            request.trace.request_metadata["transport"] = topology["transport"].clone();
        }

        Ok(())
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn requested_chain_positions() -> Value {
        json!([
            {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_000,
                "block_hash": "0xabc123"
            }
        ])
    }

    fn version_boundary(resource_id: Uuid) -> Value {
        json!({
            "logical_name_id": "ens:alice.eth",
            "resource_id": resource_id.to_string(),
            "normalized_event_id": 1_200,
            "event_kind": "RecordsChanged",
            "chain_position": {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_000,
                "block_hash": "0xabc123",
                "timestamp": "2024-06-01T00:00:17Z",
            }
        })
    }

    fn manifest_versions() -> Value {
        json!([
            {
                "source_family": ENS_EXECUTION_SOURCE_FAMILY,
                "manifest_version": 1
            },
            {
                "source_manifest_id": 7,
                "manifest_version": 3
            }
        ])
    }

    fn basenames_manifest_versions() -> Value {
        json!([
            {
                "source_family": BASENAMES_EXECUTION_SOURCE_FAMILY,
                "manifest_version": 2
            },
            {
                "source_manifest_id": 9,
                "manifest_version": 4
            }
        ])
    }

    fn basenames_requested_chain_positions() -> Value {
        json!([
            {
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_number": 31_000_000,
                "block_hash": "0xbase123"
            },
            {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "block_number": 21_000_000,
                "block_hash": "0xl1abc"
            }
        ])
    }

    fn basenames_version_boundary(resource_id: Uuid) -> Value {
        json!({
            "logical_name_id": "basenames:alice.base.eth",
            "resource_id": resource_id.to_string(),
            "normalized_event_id": 1_300,
            "event_kind": "RecordVersionChanged",
            "chain_position": {
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_number": 31_000_000,
                "block_hash": "0xbase123",
                "timestamp": "2024-06-01T00:00:17Z",
            }
        })
    }

    fn raw_call_snapshot() -> RawCallSnapshot {
        RawCallSnapshot {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xabc123".to_owned(),
            block_number: 21_000_000,
            request_hash: "0xreq-a".to_owned(),
            request_payload: json!({
                "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                "data": "0x9061b923"
            }),
            response_hash: "0xresp-a".to_owned(),
            response_payload: json!({
                "result": "0x00000000000000000000000000000000000000aa"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }
    }

    fn alias_target(resource_id: Uuid) -> Value {
        json!({
            "logical_name_id": "ens:profile.alice.eth",
            "namespace": ENS_NAMESPACE,
            "normalized_name": "profile.alice.eth",
            "canonical_display_name": "Profile.alice.eth",
            "namehash": "namehash:profile.alice.eth",
            "resource_id": resource_id.to_string(),
            "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND
        })
    }

    fn wildcard_source_ref(resource_id: Uuid) -> Value {
        json!({
            "logical_name_id": "ens:eth",
            "namespace": ENS_NAMESPACE,
            "normalized_name": "eth",
            "canonical_display_name": "Eth",
            "namehash": "namehash:eth",
            "resource_id": resource_id.to_string(),
            "binding_kind": OBSERVED_WILDCARD_PATH_BINDING_KIND
        })
    }

    fn boundary_resource_id(boundary: &Value) -> Uuid {
        let resource_id = boundary
            .get("resource_id")
            .and_then(Value::as_str)
            .expect("version boundary must include resource_id");
        Uuid::parse_str(resource_id).expect("version boundary resource_id must be a UUID")
    }

    fn first_boundary_manifest_version(manifest_versions: &Value) -> i64 {
        manifest_versions
            .as_array()
            .and_then(|items| items.first())
            .and_then(Value::as_object)
            .and_then(|item| item.get("manifest_version"))
            .and_then(Value::as_i64)
            .unwrap_or(1)
    }

    fn projection_chain_positions_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> Value {
        let boundary_timestamps = [
            request
                .outcome
                .cache_key
                .topology_version_boundary
                .get("chain_position"),
            request
                .outcome
                .cache_key
                .record_version_boundary
                .get("chain_position"),
        ];
        let positions = request
            .outcome
            .cache_key
            .requested_chain_positions
            .as_array()
            .expect("requested_chain_positions must be an array");
        let mut chain_positions = Map::new();
        for position in positions {
            let chain_id = position
                .get("chain_id")
                .and_then(Value::as_str)
                .expect("requested chain position must include chain_id");
            let block_number = position
                .get("block_number")
                .and_then(Value::as_i64)
                .expect("requested chain position must include block_number");
            let block_hash = position
                .get("block_hash")
                .and_then(Value::as_str)
                .expect("requested chain position must include block_hash");
            let timestamp = boundary_timestamps
                .iter()
                .flatten()
                .find(|candidate| {
                    candidate
                        .get("chain_id")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == chain_id)
                        && candidate
                            .get("block_hash")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == block_hash)
                        && candidate
                            .get("block_number")
                            .and_then(Value::as_i64)
                            .is_some_and(|value| value == block_number)
                })
                .and_then(|candidate| candidate.get("timestamp"))
                .and_then(Value::as_str)
                .unwrap_or("2026-04-23T00:00:00Z");
            let key = match chain_id {
                ETHEREUM_MAINNET_CHAIN_ID => "ethereum",
                BASE_MAINNET_CHAIN_ID => "base",
                other => other,
            };
            chain_positions.insert(
                key.to_owned(),
                json!({
                    "chain_id": chain_id,
                    "block_number": block_number,
                    "block_hash": block_hash,
                    "timestamp": timestamp
                }),
            );
        }
        Value::Object(chain_positions)
    }

    fn name_ref_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
    ) -> Value {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        json!({
            "logical_name_id": format!("{}:{}", request.trace.namespace, requested_selectors.surface),
            "namespace": request.trace.namespace,
            "normalized_name": requested_selectors.surface,
            "canonical_display_name": requested_selectors.surface,
            "namehash": format!("namehash:{}", requested_selectors.surface),
            "resource_id": resource_id.to_string(),
            "binding_kind": binding_kind.as_str()
        })
    }

    fn resolver_path_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
        resolver_chain_id: &str,
        resolver_address: &str,
    ) -> Value {
        let wildcard = persisted_trace_detail_object(&request.trace, "wildcard");
        let hop = wildcard
            .as_ref()
            .and_then(|detail| detail.get("source"))
            .cloned()
            .unwrap_or_else(|| name_ref_for_request(request, resource_id, binding_kind));
        let mut hop = hop
            .as_object()
            .cloned()
            .expect("resolver_path hop must be an object");
        hop.insert("chain_id".to_owned(), json!(resolver_chain_id));
        hop.insert("address".to_owned(), json!(resolver_address));
        hop.insert("latest_event_kind".to_owned(), Value::Null);
        Value::Array(vec![Value::Object(hop)])
    }

    fn projected_topology_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
        resolver_chain_id: &str,
        resolver_address: &str,
    ) -> Value {
        let alias = super::normalize_alias_detail(
            persisted_trace_detail_object(&request.trace, "alias").as_ref(),
            &request.trace.namespace,
        )
        .expect("request alias detail must normalize");
        let wildcard = super::normalize_wildcard_detail(
            persisted_trace_detail_object(&request.trace, "wildcard").as_ref(),
            &request.trace.namespace,
        )
        .expect("request wildcard detail must normalize");
        let transport = super::normalize_transport_detail(
            persisted_trace_detail_object(&request.trace, "transport").as_ref(),
        )
        .expect("request transport detail must normalize");

        json!({
            "registry_path": [name_ref_for_request(request, resource_id, binding_kind)],
            "subregistry_path": [],
            "resolver_path": resolver_path_for_request(
                request,
                resource_id,
                binding_kind,
                resolver_chain_id,
                resolver_address,
            ),
            "wildcard": wildcard,
            "alias": alias,
            "version_boundaries": {
                "topology_version_boundary": request.outcome.cache_key.topology_version_boundary.clone(),
                "record_version_boundary": request.outcome.cache_key.record_version_boundary.clone(),
            },
            "transport": transport,
        })
    }

    fn binding_kind_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> SurfaceBindingKind {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        match requested_selectors.binding_kind.as_deref() {
            None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {
                SurfaceBindingKind::DeclaredRegistryPath
            }
            Some(RESOLVER_ALIAS_PATH_BINDING_KIND) => SurfaceBindingKind::ResolverAliasPath,
            Some(OBSERVED_WILDCARD_PATH_BINDING_KIND) => SurfaceBindingKind::ObservedWildcardPath,
            Some(other) => panic!("unsupported binding_kind fixture {other}"),
        }
    }

    fn resolver_address_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> String {
        request
            .trace
            .request_metadata
            .get("transport")
            .and_then(|transport| transport.get("contract_address"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                request
                    .trace
                    .steps
                    .iter()
                    .find_map(|step| step.step_payload.get("resolver"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| match request.trace.namespace.as_str() {
                ENS_NAMESPACE => "0x0000000000000000000000000000000000000abc".to_owned(),
                BASENAMES_NAMESPACE => "0x0000000000000000000000000000000000000b60".to_owned(),
                other => panic!("unsupported namespace fixture {other}"),
            })
    }

    fn resolver_chain_id_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> String {
        match request.trace.namespace.as_str() {
            ENS_NAMESPACE => ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            BASENAMES_NAMESPACE => BASE_MAINNET_CHAIN_ID.to_owned(),
            other => panic!("unsupported namespace fixture {other}"),
        }
    }

    fn token_lineage_for_request(token_lineage_id: Uuid, chain_id: &str) -> TokenLineage {
        TokenLineage {
            token_lineage_id,
            chain_id: chain_id.to_owned(),
            block_hash: "0xtrace-support".to_owned(),
            block_number: 21_000_000,
            provenance: json!({"source": "execution_test", "anchor": "token_lineage"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn resource_for_request(resource_id: Uuid, token_lineage_id: Uuid, chain_id: &str) -> Resource {
        Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: chain_id.to_owned(),
            block_hash: "0xtrace-support".to_owned(),
            block_number: 21_000_000,
            provenance: json!({"source": "execution_test", "anchor": "resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn name_surface_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        chain_id: &str,
    ) -> NameSurface {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        NameSurface {
            logical_name_id: format!(
                "{}:{}",
                request.trace.namespace, requested_selectors.surface
            ),
            namespace: request.trace.namespace.clone(),
            input_name: requested_selectors.surface.clone(),
            canonical_display_name: requested_selectors.surface.clone(),
            normalized_name: requested_selectors.surface.clone(),
            dns_encoded_name: requested_selectors.surface.as_bytes().to_vec(),
            namehash: format!("namehash:{}", requested_selectors.surface),
            labelhashes: vec![format!("labelhash:{}", requested_selectors.surface)],
            normalizer_version: "uts46-v1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: "0xtrace-support".to_owned(),
            block_number: 21_000_000,
            provenance: json!({"source": "execution_test", "anchor": "surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn surface_binding_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        surface_binding_id: Uuid,
        resource_id: Uuid,
        binding_kind: SurfaceBindingKind,
        chain_id: &str,
    ) -> SurfaceBinding {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        SurfaceBinding {
            surface_binding_id,
            logical_name_id: format!(
                "{}:{}",
                request.trace.namespace, requested_selectors.surface
            ),
            resource_id,
            binding_kind,
            active_from: timestamp(1_717_171_700),
            active_to: None,
            chain_id: chain_id.to_owned(),
            block_hash: "0xtrace-support".to_owned(),
            block_number: 21_000_000,
            provenance: json!({"source": "execution_test", "anchor": "binding"}),
            canonicality_state: CanonicalityState::Finalized,
        }
    }

    fn supported_resolution_name_current_row(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> NameCurrentRow {
        let requested_selectors =
            extract_requested_selectors(&request.trace).expect("request selectors must parse");
        let binding_kind = binding_kind_for_request(request);
        let resolver_chain_id = resolver_chain_id_for_request(request);
        let resolver_address = resolver_address_for_request(request);
        NameCurrentRow {
            logical_name_id: format!(
                "{}:{}",
                request.trace.namespace, requested_selectors.surface
            ),
            namespace: request.trace.namespace.clone(),
            canonical_display_name: requested_selectors.surface.clone(),
            normalized_name: requested_selectors.surface.clone(),
            namehash: format!("namehash:{}", requested_selectors.surface),
            surface_binding_id: Some(surface_binding_id),
            resource_id: Some(resource_id),
            token_lineage_id: Some(token_lineage_id),
            binding_kind: Some(binding_kind),
            declared_summary: json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "resolver": {
                    "chain_id": resolver_chain_id,
                    "address": resolver_address,
                    "latest_event_kind": "ResolverChanged"
                },
                "topology": projected_topology_for_request(
                    request,
                    resource_id,
                    binding_kind,
                    &resolver_chain_id_for_request(request),
                    &resolver_address_for_request(request),
                )
            }),
            provenance: json!({
                "normalized_event_ids": [101, 102],
                "raw_fact_refs": [{
                    "kind": "log",
                    "chain_id": resolver_chain_id_for_request(request),
                    "block_hash": "0xtrace-support"
                }],
                "manifest_versions": request.outcome.cache_key.manifest_versions.clone(),
                "execution_trace_id": null,
                "derivation_kind": "projection_apply"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": [format!("{}_supported_resolution", request.trace.namespace)],
                "unsupported_reason": null,
                "enumeration_basis": "exact_name"
            }),
            chain_positions: projection_chain_positions_for_request(request),
            canonicality_summary: {
                let mut chains = Map::new();
                for position in projection_chain_positions_for_request(request)
                    .as_object()
                    .expect("chain positions must be object")
                    .values()
                {
                    if let Some(chain_id) = position.get("chain_id").and_then(Value::as_str) {
                        chains.insert(chain_id.to_owned(), Value::String("finalized".to_owned()));
                    }
                }
                json!({
                    "status": "finalized",
                    "chains": Value::Object(chains)
                })
            },
            manifest_version: first_boundary_manifest_version(
                &request.outcome.cache_key.manifest_versions,
            ),
            last_recomputed_at: timestamp(1_717_171_717),
        }
    }

    fn record_inventory_selector_entry(query: &VerifiedQuerySummary, cacheable: bool) -> Value {
        let (record_family, selector_key) =
            selector_family_and_key(&query.record_key, &query.selector);
        json!({
            "record_key": query.record_key,
            "record_family": record_family,
            "selector_key": selector_key,
            "cacheable": cacheable
        })
    }

    fn record_inventory_entry(query: &VerifiedQuerySummary) -> Value {
        let (record_family, selector_key) =
            selector_family_and_key(&query.record_key, &query.selector);
        let mut entry = json!({
            "record_key": query.record_key,
            "record_family": record_family,
            "selector_key": selector_key,
            "status": match query.status {
                VerifiedQueryStatus::Success => "success",
                VerifiedQueryStatus::NotFound => "not_found",
                VerifiedQueryStatus::ExecutionFailed => "unsupported",
            }
        });
        match query.status {
            VerifiedQueryStatus::Success => {
                let value = match &query.selector {
                    SupportedVerifiedRecordKey::Addr { coin_type } => {
                        json!({
                            "coin_type": coin_type,
                            "value": query.value.as_deref().expect("success query must include value")
                        })
                    }
                    SupportedVerifiedRecordKey::Avatar
                    | SupportedVerifiedRecordKey::Contenthash
                    | SupportedVerifiedRecordKey::Text => json!({
                        "value": query.value.as_deref().expect("success query must include value")
                    }),
                };
                entry
                    .as_object_mut()
                    .expect("entry must be object")
                    .insert("value".to_owned(), value);
            }
            VerifiedQueryStatus::NotFound => {}
            VerifiedQueryStatus::ExecutionFailed => {
                entry.as_object_mut().expect("entry must be object").insert(
                    "unsupported_reason".to_owned(),
                    json!("value_not_retained_in_normalized_events"),
                );
            }
        }
        entry
    }

    fn supported_record_inventory_row_for_request(
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
    ) -> RecordInventoryCurrentRow {
        let mut queries =
            extract_supported_verified_queries(&request.outcome).expect("queries must parse");
        queries.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        let selectors = queries
            .iter()
            .map(|query| {
                record_inventory_selector_entry(
                    query,
                    query.status != VerifiedQueryStatus::ExecutionFailed,
                )
            })
            .collect::<Vec<_>>();
        let entries = queries
            .iter()
            .filter(|query| query.status != VerifiedQueryStatus::ExecutionFailed)
            .map(record_inventory_entry)
            .collect::<Vec<_>>();
        let chain_id =
            request.outcome.cache_key.record_version_boundary["chain_position"]["chain_id"]
                .as_str()
                .unwrap_or(ETHEREUM_MAINNET_CHAIN_ID)
                .to_owned();
        let mut chain_positions = Map::new();
        chain_positions.insert(
            chain_id.clone(),
            request.outcome.cache_key.record_version_boundary["chain_position"].clone(),
        );
        let mut canonicality_chains = Map::new();
        canonicality_chains.insert(chain_id, Value::String("finalized".to_owned()));

        RecordInventoryCurrentRow {
            resource_id,
            record_version_boundary: request.outcome.cache_key.record_version_boundary.clone(),
            enumeration_basis: json!({
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false
            }),
            selectors: Value::Array(selectors),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: Some(json!({
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": request.outcome.cache_key.record_version_boundary["chain_position"].clone()
            })),
            entries: Value::Array(entries),
            provenance: json!({
                "normalized_event_ids": [1200],
                "derivation_kind": "record_inventory_current_rebuild"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "enumeration_basis": "declared_record_inventory"
            }),
            chain_positions: Value::Object(chain_positions),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": Value::Object(canonicality_chains)
            }),
            manifest_version: first_boundary_manifest_version(
                &request.outcome.cache_key.manifest_versions,
            ),
            last_recomputed_at: timestamp(1_717_171_719),
        }
    }

    async fn seed_alias_only_name_current_rebuild_inputs(
        database: &TestDatabase,
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        let logical_name_id = logical_name_id_for_request(request);
        bigname_storage::upsert_raw_blocks(
            database.pool(),
            &[
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsurface", 98, 1_717_171_698),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresource", 99, 1_717_171_699),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 101, 1_717_171_701),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xalias", 102, 1_717_171_702),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbinding-alias",
                    103,
                    1_717_171_703,
                ),
            ],
        )
        .await?;
        bigname_storage::upsert_name_surfaces(
            database.pool(),
            &[name_surface_for_request(request, ETHEREUM_MAINNET_CHAIN_ID)],
        )
        .await?;
        bigname_storage::upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_alias_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_resources(
            database.pool(),
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_alias_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_surface_bindings(
            database.pool(),
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.clone(),
                resource_id,
                binding_kind: SurfaceBindingKind::ResolverAliasPath,
                active_from: timestamp(1_717_171_703),
                active_to: None,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbinding-alias".to_owned(),
                block_number: 103,
                provenance: json!({"seed": "execution_alias_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_normalized_events(
            database.pool(),
            &[
                NormalizedEvent {
                    event_identity: "execution:alias-resolver".to_owned(),
                    namespace: ENS_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "ResolverChanged".to_owned(),
                    source_family: "ens_v1_unwrapped_authority".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(101),
                    block_hash: Some("0xresolver".to_owned()),
                    transaction_hash: Some("0xtxresolver".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:alias-resolver"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "resolver": "0x0000000000000000000000000000000000000abc",
                        "namehash": "namehash:alice.eth",
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:alias-changed".to_owned(),
                    namespace: ENS_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "AliasChanged".to_owned(),
                    source_family: "ens_v2_resolver".to_owned(),
                    manifest_version: 5,
                    source_manifest_id: None,
                    chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xalias".to_owned()),
                    transaction_hash: Some("0xtxalias".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:alias-changed"}),
                    derivation_kind: "ens_v2_resolver".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "active": true,
                        "alias_state": "active",
                        "to_name": "profile.alice.eth",
                        "to_logical_name_id": "ens:profile.alice.eth",
                        "to_normalized_name": "profile.alice.eth",
                        "to_canonical_display_name": "profile.alice.eth",
                        "to_namehash": "namehash:profile.alice.eth",
                        "to_resource_id": resource_id.to_string(),
                    }),
                },
            ],
        )
        .await?;
        rebuild_name_current_projection(database, &logical_name_id).await
    }

    async fn seed_wildcard_name_current_rebuild_inputs(
        database: &TestDatabase,
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        let logical_name_id = logical_name_id_for_request(request);
        let wildcard = persisted_trace_detail_object(&request.trace, "wildcard")
            .context("wildcard-derived execution request must include wildcard detail")?;
        let source = wildcard
            .get("source")
            .cloned()
            .context("wildcard-derived execution request must include wildcard source")?;
        let source_logical_name_id = source
            .get("logical_name_id")
            .and_then(Value::as_str)
            .unwrap_or("ens:eth");
        let source_namespace = source
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or(ENS_NAMESPACE);
        let source_normalized_name = source
            .get("normalized_name")
            .and_then(Value::as_str)
            .unwrap_or("eth");
        let source_canonical_display_name = source
            .get("canonical_display_name")
            .and_then(Value::as_str)
            .unwrap_or(source_normalized_name);
        let source_namehash = source
            .get("namehash")
            .and_then(Value::as_str)
            .unwrap_or("namehash:eth");
        let source_resource_id = source
            .get("resource_id")
            .and_then(Value::as_str)
            .map(Uuid::parse_str)
            .transpose()
            .context("wildcard source resource_id must be a UUID")?
            .unwrap_or(resource_id);
        let source_token_lineage_id = Uuid::from_u128(source_resource_id.as_u128() + 1);
        let source_surface_binding_id = Uuid::from_u128(source_resource_id.as_u128() + 2);
        bigname_storage::upsert_raw_blocks(
            database.pool(),
            &[
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xsource-surface",
                    96,
                    1_717_171_696,
                ),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsurface", 98, 1_717_171_698),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresource", 99, 1_717_171_699),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xsource-resource",
                    100,
                    1_717_171_700,
                ),
                raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 101, 1_717_171_701),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xsource-record-version",
                    102,
                    1_717_171_702,
                ),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbinding-wildcard",
                    103,
                    1_717_171_703,
                ),
            ],
        )
        .await?;
        bigname_storage::upsert_name_surfaces(
            database.pool(),
            &[
                name_surface_for_request(request, ETHEREUM_MAINNET_CHAIN_ID),
                NameSurface {
                    logical_name_id: source_logical_name_id.to_owned(),
                    namespace: source_namespace.to_owned(),
                    input_name: source_normalized_name.to_owned(),
                    canonical_display_name: source_canonical_display_name.to_owned(),
                    normalized_name: source_normalized_name.to_owned(),
                    dns_encoded_name: source_normalized_name.as_bytes().to_vec(),
                    namehash: source_namehash.to_owned(),
                    labelhashes: vec![format!("labelhash:{source_normalized_name}")],
                    normalizer_version: "uts46-v1".to_owned(),
                    normalization_warnings: json!([]),
                    normalization_errors: json!([]),
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xsource-surface".to_owned(),
                    block_number: 96,
                    provenance: json!({"seed": "execution_wildcard_source_surface"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;
        bigname_storage::upsert_token_lineages(
            database.pool(),
            &[
                TokenLineage {
                    token_lineage_id,
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xresource".to_owned(),
                    block_number: 99,
                    provenance: json!({"seed": "execution_wildcard_token_lineage"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
                TokenLineage {
                    token_lineage_id: source_token_lineage_id,
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xsource-resource".to_owned(),
                    block_number: 100,
                    provenance: json!({"seed": "execution_wildcard_source_token_lineage"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;
        bigname_storage::upsert_resources(
            database.pool(),
            &[
                Resource {
                    resource_id,
                    token_lineage_id: Some(token_lineage_id),
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xresource".to_owned(),
                    block_number: 99,
                    provenance: json!({"seed": "execution_wildcard_resource"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
                Resource {
                    resource_id: source_resource_id,
                    token_lineage_id: Some(source_token_lineage_id),
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xsource-resource".to_owned(),
                    block_number: 100,
                    provenance: json!({"seed": "execution_wildcard_source_resource"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;
        bigname_storage::upsert_surface_bindings(
            database.pool(),
            &[
                SurfaceBinding {
                    surface_binding_id: source_surface_binding_id,
                    logical_name_id: source_logical_name_id.to_owned(),
                    resource_id: source_resource_id,
                    binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                    active_from: timestamp(1_717_171_700),
                    active_to: None,
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xsource-resource".to_owned(),
                    block_number: 100,
                    provenance: json!({"seed": "execution_wildcard_source_binding"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
                SurfaceBinding {
                    surface_binding_id,
                    logical_name_id: logical_name_id.clone(),
                    resource_id,
                    binding_kind: SurfaceBindingKind::ObservedWildcardPath,
                    active_from: timestamp(1_717_171_703),
                    active_to: None,
                    chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                    block_hash: "0xbinding-wildcard".to_owned(),
                    block_number: 103,
                    provenance: json!({"seed": "execution_wildcard_binding"}),
                    canonicality_state: CanonicalityState::Canonical,
                },
            ],
        )
        .await?;
        bigname_storage::upsert_normalized_events(
            database.pool(),
            &[
                NormalizedEvent {
                    event_identity: "execution:wildcard-source-resolver".to_owned(),
                    namespace: ENS_NAMESPACE.to_owned(),
                    logical_name_id: Some(source_logical_name_id.to_owned()),
                    resource_id: Some(source_resource_id),
                    event_kind: "ResolverChanged".to_owned(),
                    source_family: "ens_v1_unwrapped_authority".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(101),
                    block_hash: Some("0xresolver".to_owned()),
                    transaction_hash: Some("0xtxwildcardsourceresolver".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:wildcard-source-resolver"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "resolver": "0x0000000000000000000000000000000000000def",
                        "namehash": source_namehash,
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:wildcard-source-record-version".to_owned(),
                    namespace: ENS_NAMESPACE.to_owned(),
                    logical_name_id: Some(source_logical_name_id.to_owned()),
                    resource_id: Some(source_resource_id),
                    event_kind: "RecordVersionChanged".to_owned(),
                    source_family: "ens_v1_unwrapped_authority".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xsource-record-version".to_owned()),
                    transaction_hash: Some("0xtxwildcardsourceversion".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:wildcard-source-record-version"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({"record_version": 6}),
                    after_state: json!({"record_version": 7}),
                },
            ],
        )
        .await?;
        rebuild_name_current_projection(database, &logical_name_id).await
    }

    async fn insert_basenames_execution_manifest(pool: &PgPool) -> Result<i64> {
        let manifest_id = sqlx::query(
            r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain,
                    deployment_epoch,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
                RETURNING manifest_id
            "#,
        )
        .bind(2_i64)
        .bind(BASENAMES_NAMESPACE)
        .bind("basenames_execution")
        .bind(ETHEREUM_MAINNET_CHAIN_ID)
        .bind("basenames_v1")
        .bind("active")
        .bind("ensip15@2026-04-16")
        .bind("tests/execution/basenames_execution-v2.toml")
        .bind("{}")
        .fetch_one(pool)
        .await?
        .try_get("manifest_id")?;
        sqlx::query(
            r#"
                INSERT INTO manifest_capability_flags (
                    manifest_id,
                    capability_name,
                    status,
                    notes
                )
                VALUES ($1, $2, $3::capability_support_status, $4)
            "#,
        )
        .bind(manifest_id)
        .bind("verified_resolution")
        .bind("supported")
        .bind(None::<String>)
        .execute(pool)
        .await?;
        insert_basenames_execution_manifest_contract(pool, manifest_id).await?;
        Ok(manifest_id)
    }

    async fn insert_basenames_execution_manifest_contract(
        pool: &PgPool,
        manifest_id: i64,
    ) -> Result<()> {
        let contract_instance_id = Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0002);
        sqlx::query(
            r#"
                INSERT INTO contract_instances (
                    contract_instance_id,
                    chain_id,
                    contract_kind,
                    provenance
                )
                VALUES ($1, $2, 'contract', $3::jsonb)
                ON CONFLICT (contract_instance_id) DO NOTHING
            "#,
        )
        .bind(contract_instance_id)
        .bind(ETHEREUM_MAINNET_CHAIN_ID)
        .bind(json!({"seed": "execution_basenames_execution"}))
        .execute(pool)
        .await
        .context("failed to insert Basenames execution contract_instance")?;

        sqlx::query(
            r#"
                INSERT INTO manifest_contract_instances (
                    manifest_id,
                    declaration_kind,
                    declaration_name,
                    contract_instance_id,
                    declared_address,
                    role,
                    proxy_kind
                )
                VALUES (
                    $1,
                    'contract',
                    'l1_resolver',
                    $2,
                    '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31',
                    'l1_resolver',
                    'none'
                )
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames execution manifest_contract_instance")?;

        Ok(())
    }

    async fn insert_chain_checkpoint(
        pool: &PgPool,
        chain_id: &str,
        block_hash: &str,
        block_number: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO chain_checkpoints (
                    chain_id,
                    finalized_block_hash,
                    finalized_block_number
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (chain_id)
                DO UPDATE SET
                    finalized_block_hash = EXCLUDED.finalized_block_hash,
                    finalized_block_number = EXCLUDED.finalized_block_number
            "#,
        )
        .bind(chain_id)
        .bind(block_hash)
        .bind(block_number)
        .execute(pool)
        .await
        .with_context(|| format!("failed to insert chain checkpoint for {chain_id}"))?;

        Ok(())
    }

    async fn seed_basenames_name_current_rebuild_inputs(
        database: &TestDatabase,
        request: &PersistEnsExactNameVerifiedResolutionRequest,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        let logical_name_id = logical_name_id_for_request(request);
        bigname_storage::upsert_raw_blocks(
            database.pool(),
            &[
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-surface", 98, 1_717_171_698),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resource", 99, 1_717_171_699),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-grant", 101, 1_717_171_701),
                raw_block(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-authority",
                    102,
                    1_717_171_702,
                ),
                raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resolver", 103, 1_717_171_703),
                raw_block(
                    BASE_MAINNET_CHAIN_ID,
                    "0xbase-binding-supported",
                    104,
                    1_717_171_704,
                ),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1-compatible",
                    21_000_099,
                    1_717_171_704,
                ),
                raw_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1",
                    21_000_100,
                    1_776_387_700,
                ),
            ],
        )
        .await?;
        insert_chain_checkpoint(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xbasenamesl1",
            21_000_100,
        )
        .await?;
        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1-compatible",
                    21_000_099,
                    1_717_171_704,
                ),
                chain_lineage_block(
                    ETHEREUM_MAINNET_CHAIN_ID,
                    "0xbasenamesl1",
                    21_000_100,
                    1_776_387_700,
                ),
            ],
        )
        .await?;
        bigname_storage::upsert_name_surfaces(
            database.pool(),
            &[NameSurface {
                logical_name_id: logical_name_id.clone(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                input_name: "alice.base.eth".to_owned(),
                canonical_display_name: "Alice.base.eth".to_owned(),
                normalized_name: "alice.base.eth".to_owned(),
                dns_encoded_name: b"alice.base.eth".to_vec(),
                namehash: "namehash:alice.base.eth".to_owned(),
                labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-surface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "execution_basenames_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_token_lineages(
            database.pool(),
            &[TokenLineage {
                token_lineage_id,
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_basenames_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_resources(
            database.pool(),
            &[Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-resource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_basenames_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_surface_bindings(
            database.pool(),
            &[SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.clone(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_704),
                active_to: None,
                chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbase-binding-supported".to_owned(),
                block_number: 104,
                provenance: json!({"seed": "execution_basenames_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        bigname_storage::upsert_normalized_events(
            database.pool(),
            &[
                NormalizedEvent {
                    event_identity: "execution:basenames:grant".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "RegistrationGranted".to_owned(),
                    source_family: "basenames_base_registrar".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(101),
                    block_hash: Some("0xbase-grant".to_owned()),
                    transaction_hash: Some("0xtxbasegrant".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:grant"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "authority_kind": "registrar",
                        "authority_key": "registrar:base-mainnet:alice",
                        "registrant": "0x00000000000000000000000000000000000000aa",
                        "expiry": 1_900_000_000_i64,
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:basenames:authority".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "AuthorityTransferred".to_owned(),
                    source_family: "basenames_base_registry".to_owned(),
                    manifest_version: 3,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(102),
                    block_hash: Some("0xbase-authority".to_owned()),
                    transaction_hash: Some("0xtxbaseauthority".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:authority"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "owner": "0x00000000000000000000000000000000000000bb",
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:basenames:resolver".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "ResolverChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(103),
                    block_hash: Some("0xbase-resolver".to_owned()),
                    transaction_hash: Some("0xtxbaseresolver".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:resolver"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "resolver": "0x0000000000000000000000000000000000000abc",
                        "namehash": "namehash:alice.base.eth",
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:basenames:record-version".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordVersionChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(104),
                    block_hash: Some("0xbase-binding-supported".to_owned()),
                    transaction_hash: Some("0xtxbaserecordversion".to_owned()),
                    log_index: Some(0),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:record-version"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({
                        "record_version": 6,
                    }),
                    after_state: json!({
                        "record_version": 7,
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:basenames:addr".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(104),
                    block_hash: Some("0xbase-binding-supported".to_owned()),
                    transaction_hash: Some("0xtxbaseaddr".to_owned()),
                    log_index: Some(1),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:addr"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                    }),
                },
                NormalizedEvent {
                    event_identity: "execution:basenames:text".to_owned(),
                    namespace: BASENAMES_NAMESPACE.to_owned(),
                    logical_name_id: Some(logical_name_id.clone()),
                    resource_id: Some(resource_id),
                    event_kind: "RecordChanged".to_owned(),
                    source_family: "basenames_base_resolver".to_owned(),
                    manifest_version: 4,
                    source_manifest_id: None,
                    chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                    block_number: Some(104),
                    block_hash: Some("0xbase-binding-supported".to_owned()),
                    transaction_hash: Some("0xtxbasetext".to_owned()),
                    log_index: Some(2),
                    raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:text"}),
                    derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                    canonicality_state: CanonicalityState::Canonical,
                    before_state: json!({}),
                    after_state: json!({
                        "record_key": "text",
                        "record_family": "text",
                        "selector_key": null,
                    }),
                },
            ],
        )
        .await?;
        insert_basenames_execution_manifest(database.pool()).await?;
        rebuild_name_current_projection(database, &logical_name_id).await
    }

    async fn seed_supported_resolution_storage_from_rebuild(
        database: &TestDatabase,
        request: &mut PersistEnsExactNameVerifiedResolutionRequest,
    ) -> Result<()> {
        let resource_id = boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
        let token_lineage_id = Uuid::from_u128(resource_id.as_u128() + 1);
        let surface_binding_id = Uuid::from_u128(resource_id.as_u128() + 2);
        match (
            request.trace.namespace.as_str(),
            binding_kind_for_request(request),
        ) {
            (ENS_NAMESPACE, SurfaceBindingKind::ResolverAliasPath) => {
                seed_alias_only_name_current_rebuild_inputs(
                    database,
                    request,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            }
            (ENS_NAMESPACE, SurfaceBindingKind::ObservedWildcardPath) => {
                seed_wildcard_name_current_rebuild_inputs(
                    database,
                    request,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            }
            (BASENAMES_NAMESPACE, SurfaceBindingKind::DeclaredRegistryPath) => {
                seed_basenames_name_current_rebuild_inputs(
                    database,
                    request,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            }
            (namespace, binding_kind) => {
                bail!(
                    "no rebuild-backed supported-resolution seed for namespace {namespace} binding_kind {}",
                    binding_kind.as_str()
                );
            }
        }

        let logical_name_id = logical_name_id_for_request(request);
        let row = bigname_storage::load_name_current(database.pool(), &logical_name_id)
            .await?
            .with_context(|| {
                format!("rebuilt execution test name_current row missing for {logical_name_id}")
            })?;
        sync_request_with_rebuilt_name_current(request, &row)?;
        let record_resource_id =
            boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
        upsert_record_inventory_current_rows(
            database.pool(),
            &[supported_record_inventory_row_for_request(
                request,
                record_resource_id,
            )],
        )
        .await?;
        Ok(())
    }

    async fn seed_supported_resolution_storage(
        database: &TestDatabase,
        request: &PersistEnsExactNameVerifiedResolutionRequest,
    ) -> Result<()> {
        let resource_id = boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
        let token_lineage_id = Uuid::from_u128(resource_id.as_u128() + 1);
        let surface_binding_id = Uuid::from_u128(resource_id.as_u128() + 2);
        let primary_chain_id = request
            .outcome
            .cache_key
            .record_version_boundary
            .get("chain_position")
            .and_then(|chain_position| chain_position.get("chain_id"))
            .and_then(Value::as_str)
            .unwrap_or(ETHEREUM_MAINNET_CHAIN_ID);
        let binding_kind = binding_kind_for_request(request);

        upsert_token_lineages(
            database.pool(),
            &[token_lineage_for_request(
                token_lineage_id,
                primary_chain_id,
            )],
        )
        .await?;
        upsert_resources(
            database.pool(),
            &[resource_for_request(
                resource_id,
                token_lineage_id,
                primary_chain_id,
            )],
        )
        .await?;
        upsert_name_surfaces(
            database.pool(),
            &[name_surface_for_request(request, primary_chain_id)],
        )
        .await?;
        upsert_surface_bindings(
            database.pool(),
            &[surface_binding_for_request(
                request,
                surface_binding_id,
                resource_id,
                binding_kind,
                primary_chain_id,
            )],
        )
        .await?;
        upsert_name_current_rows(
            database.pool(),
            &[supported_resolution_name_current_row(
                request,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )],
        )
        .await?;
        upsert_record_inventory_current_rows(
            database.pool(),
            &[supported_record_inventory_row_for_request(
                request,
                resource_id,
            )],
        )
        .await?;
        Ok(())
    }

    fn basenames_transport_direct_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000031);
        let finished_at = timestamp(1_717_171_930);
        let request_key = "basenames:alice.base.eth:addr:60,text:com.twitter".to_owned();
        let verified_queries = json!([
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            }
        ]);

        PersistEnsExactNameVerifiedResolutionRequest {
            raw_call_snapshots: Vec::new(),
            trace: ExecutionTrace {
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                request_key: request_key.clone(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                chain_context: json!({
                    "requested_positions": basenames_requested_chain_positions(),
                    "topology_version_boundary": {
                        BASE_MAINNET_CHAIN_ID: 31_000_000
                    }
                }),
                manifest_context: json!({
                    "manifest_versions": basenames_manifest_versions(),
                    "rollout_boundary": "supported"
                }),
                contracts_called: json!([
                    {
                        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                        "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                        "selector": "0x9061b923"
                    }
                ]),
                gateway_digests: json!(["sha256:ccip-request", "sha256:ccip-response"]),
                final_payload: Some(json!({
                    "verified_queries": verified_queries.clone()
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.base.eth",
                    "record_keys": ["addr:60", "text:com.twitter"],
                    "entrypoint": BASENAMES_L1_RESOLVER_ROLE,
                    "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                    "transport": {
                        "source_chain_id": BASE_MAINNET_CHAIN_ID,
                        "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                        "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                        "latest_event_kind": null
                    }
                }),
                finished_at: Some(finished_at),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            BASE_MAINNET_CHAIN_ID: {
                                "block_hash": "0xbase123",
                                "block_number": 31_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": BASENAMES_L1_RESOLVER_ROLE,
                            "resolver": "0x0000000000000000000000000000000000000abc"
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_l1_resolver".to_owned(),
                        input_digest: Some("sha256:l1-resolver-input".to_owned()),
                        output_digest: Some("sha256:l1-resolver-output".to_owned()),
                        latency_ms: Some(18),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xl1abc",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "name": "alice.base.eth",
                            "record_count": 2
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 2,
                        step_kind: "ccip_offchain_lookup".to_owned(),
                        input_digest: Some("sha256:ccip-input".to_owned()),
                        output_digest: Some("sha256:ccip-output".to_owned()),
                        latency_ms: Some(32),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xl1abc",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "gateway_digest": "sha256:ccip-request"
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 3,
                        step_kind: "resolve_with_proof".to_owned(),
                        input_digest: Some("sha256:proof-input".to_owned()),
                        output_digest: Some("sha256:proof-output".to_owned()),
                        latency_ms: Some(11),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xl1abc",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "proof_kind": "signature"
                        }),
                    },
                ],
            },
            outcome: ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key,
                    requested_chain_positions: basenames_requested_chain_positions(),
                    manifest_versions: basenames_manifest_versions(),
                    topology_version_boundary: basenames_version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bab1,
                    )),
                    record_version_boundary: basenames_version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bab2,
                    )),
                },
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": verified_queries
                })),
                failure_payload: None,
                finished_at,
            },
        }
    }

    fn success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000011);
        let finished_at = timestamp(1_717_171_717);
        let request_key = "ens:alice.eth:addr:60".to_owned();
        PersistEnsExactNameVerifiedResolutionRequest {
            raw_call_snapshots: vec![raw_call_snapshot()],
            trace: ExecutionTrace {
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                request_key: request_key.clone(),
                namespace: ENS_NAMESPACE.to_owned(),
                chain_context: json!({
                    "requested_positions": requested_chain_positions(),
                    "topology_version_boundary": {
                        ETHEREUM_MAINNET_CHAIN_ID: 21_000_000
                    }
                }),
                manifest_context: json!({
                    "manifest_versions": manifest_versions(),
                    "rollout_boundary": "shadow"
                }),
                contracts_called: json!([
                    {
                        "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "selector": "0x9061b923"
                    }
                ]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "record_kind": "addr",
                    "coin_type": 60,
                    "value": "0x00000000000000000000000000000000000000aa"
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.eth",
                    "record_key": "addr:60",
                    "normalizer_version": "uts46-v1"
                }),
                finished_at: Some(finished_at),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xabc123",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
                            "resolver": ENS_UNIVERSAL_RESOLVER_ADDRESS
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_universal_resolver".to_owned(),
                        input_digest: Some("sha256:resolver-input".to_owned()),
                        output_digest: Some("sha256:resolver-output".to_owned()),
                        latency_ms: Some(28),
                        canonicality_dependency: json!({
                            ETHEREUM_MAINNET_CHAIN_ID: {
                                "block_hash": "0xabc123",
                                "block_number": 21_000_000,
                                "state": "canonical"
                            }
                        }),
                        step_payload: json!({
                            "coin_type": 60,
                            "name": "alice.eth",
                            "resolved_address": "0x00000000000000000000000000000000000000aa"
                        }),
                    },
                ],
            },
            outcome: ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key,
                    requested_chain_positions: requested_chain_positions(),
                    manifest_versions: manifest_versions(),
                    topology_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000aaa1,
                    )),
                    record_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000aaa2,
                    )),
                },
                execution_trace_id,
                request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": [
                        {
                            "record_key": "addr:60",
                            "status": "success",
                            "value": {
                                "coin_type": "60",
                                "value": "0x00000000000000000000000000000000000000aa"
                            }
                        }
                    ]
                })),
                failure_payload: None,
                finished_at,
            },
        }
    }

    fn execution_failed_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        request.raw_call_snapshots.clear();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000012);
        request.trace.request_key = "ens:alice.eth:addr:60".to_owned();
        request.trace.final_payload = None;
        request.trace.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "stage": "call_universal_resolver"
        }));
        request.trace.finished_at = Some(timestamp(1_717_171_800));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "execution_failed",
                    "failure_reason": "resolver_call_reverted"
                }
            ]
        }));
        request.outcome.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "reverted": true
        }));
        request.outcome.finished_at = request
            .trace
            .finished_at
            .expect("execution failed test trace must finish");
        request
    }

    fn contenthash_success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000018);
        let request_key = "ens:alice.eth:contenthash".to_owned();
        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "record_kind": "contenthash",
            "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
        }));
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "contenthash",
            "normalizer_version": "uts46-v1"
        });
        request.trace.steps[1].step_payload = json!({
            "name": "alice.eth",
            "contenthash": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
        });
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "contenthash",
                    "status": "success",
                    "value": {
                        "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
                    }
                }
            ]
        }));
        request
    }

    fn avatar_success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000027);
        let request_key = "ens:alice.eth:avatar".to_owned();
        let avatar = "https://cdn.example.test/alice.png";
        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "record_kind": "avatar",
            "value": avatar
        }));
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "avatar",
            "normalizer_version": "uts46-v1"
        });
        request.trace.steps[1].step_payload = json!({
            "name": "alice.eth",
            "avatar": avatar
        });
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": avatar
                    }
                }
            ]
        }));
        request
    }

    fn contenthash_not_found_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = contenthash_success_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000019);
        request.trace.final_payload = Some(json!({
            "failure_reason": "no_contenthash_record"
        }));
        request.trace.finished_at = Some(timestamp(1_717_171_760));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "contenthash",
                    "status": "not_found",
                    "failure_reason": "no_contenthash_record"
                }
            ]
        }));
        request.outcome.finished_at = request
            .trace
            .finished_at
            .expect("contenthash not_found test trace must finish");
        request
    }

    fn contenthash_execution_failed_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = contenthash_success_request();
        request.raw_call_snapshots.clear();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001a);
        request.trace.final_payload = None;
        request.trace.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "stage": "call_universal_resolver"
        }));
        request.trace.finished_at = Some(timestamp(1_717_171_810));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "contenthash",
                    "status": "execution_failed",
                    "failure_reason": "resolver_call_reverted"
                }
            ]
        }));
        request.outcome.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "reverted": true
        }));
        request.outcome.finished_at = request
            .trace
            .finished_at
            .expect("contenthash execution_failed test trace must finish");
        request
    }

    fn multi_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        let ordered_record_keys = vec![
            "addr:60".to_owned(),
            "addr:0".to_owned(),
            "addr:2".to_owned(),
        ];
        let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000014);
        let finished_at = timestamp(1_717_171_900);
        let verified_queries = json!([
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            },
            {
                "record_key": "addr:0",
                "status": "not_found",
                "failure_reason": "no_addr_record"
            },
            {
                "record_key": "addr:2",
                "status": "execution_failed",
                "failure_reason": "resolver_call_reverted"
            }
        ]);

        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "verified_queries": verified_queries.clone()
        }));
        request.trace.failure_payload = None;
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": ordered_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.finished_at = Some(finished_at);
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": verified_queries
        }));
        request.outcome.failure_payload = None;
        request.outcome.finished_at = finished_at;
        request
    }

    fn contenthash_mixed_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = contenthash_success_request();
        let ordered_record_keys = vec![
            "text:com.twitter".to_owned(),
            "contenthash".to_owned(),
            "addr:60".to_owned(),
        ];
        let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001b);
        let finished_at = timestamp(1_717_171_920);
        let verified_queries = json!([
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            }
        ]);

        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "verified_queries": verified_queries.clone()
        }));
        request.trace.failure_payload = None;
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": ordered_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.finished_at = Some(finished_at);
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": verified_queries
        }));
        request.outcome.failure_payload = None;
        request.outcome.finished_at = finished_at;
        request
    }

    fn avatar_mixed_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = avatar_success_request();
        let ordered_record_keys = vec![
            "avatar".to_owned(),
            "text:com.twitter".to_owned(),
            "contenthash".to_owned(),
            "addr:60".to_owned(),
        ];
        let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000028);
        let finished_at = timestamp(1_717_171_930);
        let avatar = "https://cdn.example.test/alice.png";
        let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
        let verified_queries = json!([
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": avatar
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            }
        ]);

        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "verified_queries": verified_queries.clone()
        }));
        request.trace.failure_payload = None;
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": ordered_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.finished_at = Some(finished_at);
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": verified_queries
        }));
        request.outcome.failure_payload = None;
        request.outcome.finished_at = finished_at;
        request
    }

    fn alias_only_text_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = success_request();
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001c);
        let request_key =
            normalized_request_key(ENS_NAMESPACE, "alice.eth", &["text:com.twitter".to_owned()]);
        let alias_target = alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab3));

        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "record_kind": "text",
            "value": "@alice-via-alias"
        }));
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "text:com.twitter",
            "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND,
            "alias": {
                "final_target": alias_target.clone(),
                "hops": [alias_target.clone()]
            },
            "normalizer_version": "uts46-v1"
        });
        request.trace.steps[1].step_payload = json!({
            "name": "alice.eth",
            "text_key": "com.twitter",
            "value": "@alice-via-alias"
        });
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    }
                }
            ]
        }));
        request
    }

    fn alias_only_avatar_request() -> PersistEnsExactNameVerifiedResolutionRequest {
        let mut request = avatar_success_request();
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000029);
        let request_key =
            normalized_request_key(ENS_NAMESPACE, "alice.eth", &["avatar".to_owned()]);
        let alias_target = alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab5));
        let avatar = "https://cdn.example.test/alice-via-alias.png";

        request.trace.execution_trace_id = execution_trace_id;
        request.trace.request_key = request_key.clone();
        request.trace.final_payload = Some(json!({
            "record_kind": "avatar",
            "value": avatar
        }));
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "avatar",
            "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND,
            "alias": {
                "final_target": alias_target.clone(),
                "hops": [alias_target.clone()]
            },
            "normalizer_version": "uts46-v1"
        });
        request.trace.steps[1].step_payload = json!({
            "name": "alice.eth",
            "avatar": avatar
        });
        request.outcome.cache_key.request_key = request_key;
        request.outcome.execution_trace_id = execution_trace_id;
        request.outcome.outcome_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": avatar
                    }
                }
            ]
        }));
        request
    }

    #[tokio::test]
    async fn persists_successful_direct_path_and_reads_back_storage_identity() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = success_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        let loaded_raw_calls = load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?;
        assert_eq!(loaded_raw_calls, request.raw_call_snapshots);

        let persisted_again =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(persisted_again, persisted);
        assert_eq!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?,
            request.raw_call_snapshots
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_successful_direct_path_with_one_pool_connection() -> Result<()> {
        let database = TestDatabase::new_with_max_connections(1).await?;
        let request = success_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted = timeout(
            Duration::from_secs(5),
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request),
        )
        .await
        .expect("direct-path persistence should not block waiting for a second pool checkout")?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );
        assert_eq!(
            load_execution_trace(database.pool(), persisted.execution_trace_id).await?,
            Some(request.trace.clone())
        );
        assert_eq!(
            load_execution_outcome(database.pool(), &persisted.cache_key).await?,
            Some(request.outcome.clone())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_avatar_success_direct_path_and_reads_back_storage_identity() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = avatar_success_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after avatar persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after avatar persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_multi_selector_direct_path_with_ordered_mixed_results() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = multi_selector_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );
        assert_eq!(
            persisted.cache_key.request_key,
            "ens:alice.eth:addr:0,addr:2,addr:60"
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        let loaded_verified_queries = loaded_outcome
            .outcome_payload
            .as_ref()
            .and_then(|payload| payload.get("verified_queries"))
            .and_then(Value::as_array)
            .expect("verified_queries must be present");
        let ordered_record_keys = loaded_verified_queries
            .iter()
            .filter_map(|query| query.get("record_key"))
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert_eq!(ordered_record_keys, vec!["addr:60", "addr:0", "addr:2"]);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_mixed_selector_direct_path_with_avatar_and_preserves_query_order()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = avatar_mixed_selector_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted.cache_key.request_key,
            "ens:alice.eth:addr:60,avatar,contenthash,text:com.twitter"
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after avatar mixed persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after avatar mixed persistence");
        assert_eq!(loaded_outcome, request.outcome);

        let loaded_verified_queries = loaded_outcome
            .outcome_payload
            .as_ref()
            .and_then(|payload| payload.get("verified_queries"))
            .and_then(Value::as_array)
            .expect("verified_queries must be present");
        let ordered_record_keys = loaded_verified_queries
            .iter()
            .filter_map(|query| query.get("record_key"))
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_record_keys,
            vec!["avatar", "text:com.twitter", "contenthash", "addr:60"]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_execution_failed_direct_path_without_raw_call_snapshots() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = execution_failed_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted.execution_trace_id,
            request.trace.execution_trace_id
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "execution failed direct path fixture should not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_contenthash_success_direct_path() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = contenthash_success_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_contenthash_not_found_direct_path() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = contenthash_not_found_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_contenthash_execution_failed_direct_path_without_raw_call_snapshots()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = contenthash_execution_failed_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted.execution_trace_id,
            request.trace.execution_trace_id
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "contenthash execution failed direct path fixture should not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_mixed_selector_direct_path_with_contenthash_and_preserves_query_order()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = contenthash_mixed_selector_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted.cache_key.request_key,
            "ens:alice.eth:addr:60,contenthash,text:com.twitter"
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after persistence");
        assert_eq!(loaded_outcome, request.outcome);

        let loaded_verified_queries = loaded_outcome
            .outcome_payload
            .as_ref()
            .and_then(|payload| payload.get("verified_queries"))
            .and_then(Value::as_array)
            .expect("verified_queries must be present");
        let ordered_record_keys = loaded_verified_queries
            .iter()
            .filter_map(|query| query.get("record_key"))
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_record_keys,
            vec!["text:com.twitter", "contenthash", "addr:60"]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_exact_surface_alias_only_path_with_resolver_alias_binding() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = alias_only_text_request();
        seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after alias-only persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after alias-only persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_exact_surface_alias_only_avatar_path_with_resolver_alias_binding()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = alias_only_avatar_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after alias-only avatar persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after alias-only avatar persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rolls_back_raw_calls_and_trace_when_outcome_write_fails() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = success_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let mut conflicting_trace = request.trace.clone();
        conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000016);
        conflicting_trace.request_type = "verified_primary_name".to_owned();
        conflicting_trace.final_payload = Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": "alice.eth"
            }
        }));
        upsert_execution_trace(database.pool(), &conflicting_trace).await?;

        let mut conflicting_outcome = request.outcome.clone();
        conflicting_outcome.execution_trace_id = conflicting_trace.execution_trace_id;
        conflicting_outcome.request_type = conflicting_trace.request_type.clone();
        conflicting_outcome.namespace = "basenames".to_owned();
        conflicting_outcome.outcome_payload = Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": "alice.eth"
            }
        }));
        upsert_execution_outcome(database.pool(), &conflicting_outcome).await?;

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("conflicting cache identity must roll back the whole direct-path write");
        assert!(
            error
                .to_string()
                .contains("execution outcome cache identity mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "failed direct-path persistence must not leave a trace row behind"
        );
        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "failed direct-path persistence must not leave raw call snapshots behind"
        );
        assert_eq!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key).await?,
            Some(conflicting_outcome),
            "the pre-existing conflicting outcome must remain untouched"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_trace_only_when_storage_revalidation_fails_closed() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = success_request();

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("missing projection inputs must fail during storage revalidation");
        assert!(
            error
                .to_string()
                .contains("supported outcome persistence failed closed after storage revalidation"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id).await?,
            Some(request.trace.clone())
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "revalidation failure must not persist outcome rows"
        );
        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "revalidation failure must not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[test]
    fn validates_stable_request_key_normalization_for_multi_selector_requests() -> Result<()> {
        let request = multi_selector_request();
        validate_direct_request(&request)?;
        assert_eq!(
            request.trace.request_key,
            "ens:alice.eth:addr:0,addr:2,addr:60"
        );

        let mut unnormalized_request = request.clone();
        unnormalized_request.trace.request_key = "ens:alice.eth:addr:60,addr:0,addr:2".to_owned();
        unnormalized_request.outcome.cache_key.request_key =
            unnormalized_request.trace.request_key.clone();

        let error = validate_direct_request(&unnormalized_request)
            .expect_err("unnormalized request_key must be rejected");
        assert!(
            error
                .to_string()
                .contains("does not match expected ens:alice.eth:addr:0,addr:2,addr:60"),
            "unexpected error: {error:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rejects_duplicate_selectors_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = multi_selector_request();
        let duplicate_record_keys = vec!["addr:60".to_owned(), "addr:60".to_owned()];
        let request_key =
            normalized_request_key(ENS_NAMESPACE, "alice.eth", &duplicate_record_keys);
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000015);
        request.trace.request_key = request_key.clone();
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": duplicate_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.final_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "not_found",
                    "failure_reason": "no_addr_record"
                }
            ]
        }));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.cache_key.request_key = request_key;
        request.outcome.outcome_payload = request.trace.final_payload.clone();

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("duplicate selectors must be rejected");
        assert!(
            error
                .to_string()
                .contains("must not contain duplicate selectors"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected request must not persist outcome rows"
        );
        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "rejected request must not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_text_selector_results_before_writing_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = multi_selector_request();
        let ordered_record_keys = vec!["addr:60".to_owned(), "text:com.twitter".to_owned()];
        let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000013);
        request.trace.request_key = request_key.clone();
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": ordered_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.final_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice"
                    }
                }
            ]
        }));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.cache_key.request_key = request_key;
        request.outcome.outcome_payload = request.trace.final_payload.clone();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert!(
            load_execution_trace(database.pool(), persisted.execution_trace_id)
                .await?
                .is_some(),
            "supported text selector must persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &persisted.cache_key)
                .await?
                .is_some(),
            "supported text selector must persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_still_unsupported_selector_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = multi_selector_request();
        let ordered_record_keys = vec!["addr:60".to_owned(), "abi".to_owned()];
        let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000017);
        request.trace.request_key = request_key.clone();
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_keys": ordered_record_keys,
            "normalizer_version": "uts46-v1"
        });
        request.trace.final_payload = Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    }
                },
                {
                    "record_key": "abi",
                    "status": "success",
                    "value": {
                        "value": "0x1234"
                    }
                }
            ]
        }));
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.cache_key.request_key = request_key;
        request.outcome.outcome_payload = request.trace.final_payload.clone();

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("unsupported selector must be rejected");
        assert!(
            error.to_string().contains(
                "only supports addr:<coin_type>, avatar, contenthash, and text:<key> selectors"
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected request must not persist outcome rows"
        );
        assert!(
            load_raw_call_snapshots_by_block_hash(
                database.pool(),
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
            )
            .await?
            .is_empty(),
            "rejected request must not persist raw call snapshots"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_basenames_path_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = avatar_success_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.steps[1].step_kind = "call_basenames_resolver".to_owned();

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("Basenames execution paths must remain unsupported");
        assert!(
            error
                .to_string()
                .contains("must not persist non-direct step call_basenames_resolver"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected Basenames path must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected Basenames path must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_linked_subregistry_binding_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = success_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001d);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "addr:60",
            "binding_kind": LINKED_SUBREGISTRY_PATH_BINDING_KIND,
            "normalizer_version": "uts46-v1"
        });

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("linked-subregistry path must remain unsupported");
        assert!(
            error
                .to_string()
                .contains("must not persist non-alias ancestor-selected binding_kind"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected linked-subregistry request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected linked-subregistry request must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_wildcard_derived_alias_only_path_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = alias_only_text_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001e);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.request_metadata["wildcard"] = json!({
            "source": alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab4)),
            "matched_labels": ["profile"]
        });

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("wildcard-derived alias-only path must remain unsupported");
        assert!(
            error
                .to_string()
                .contains("only supports wildcard.source=null with matched_labels=[]"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected wildcard-derived request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected wildcard-derived request must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_exact_surface_wildcard_derived_path_with_observed_wildcard_binding()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = success_request();
        let wildcard_source =
            wildcard_source_ref(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab6));

        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000030);
        request.trace.request_key = "ens:alice.eth:addr:60".to_owned();
        request.trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "addr:60",
            "binding_kind": OBSERVED_WILDCARD_PATH_BINDING_KIND,
            "wildcard": {
                "source": wildcard_source.clone(),
                "matched_labels": ["alice"]
            },
            "normalizer_version": "uts46-v1"
        });
        request.trace.steps.push(ExecutionTraceStep {
            step_index: 2,
            step_kind: "call_wildcard_resolver".to_owned(),
            input_digest: Some("sha256:wildcard-input".to_owned()),
            output_digest: Some("sha256:wildcard-output".to_owned()),
            latency_ms: Some(19),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "name": "alice.eth",
                "wildcard": {
                    "source": wildcard_source,
                    "matched_labels": ["alice"]
                }
            }),
        });
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.outcome.cache_key.request_key = request.trace.request_key.clone();
        request.outcome.finished_at = request
            .trace
            .finished_at
            .expect("wildcard-derived test trace must finish");
        seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

        let persisted =
            persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after wildcard-derived persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after wildcard-derived persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_transport_assisted_alias_only_path_before_writing_any_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = alias_only_text_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001f);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.request_metadata["transport"] = json!({
            "source_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "target_chain_id": "base",
            "contract_address": "0x0000000000000000000000000000000000000bad",
            "latest_event_kind": "TransportConfigured"
        });

        let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
            .await
            .expect_err("transport-assisted alias-only path must remain unsupported");
        assert!(
            error
                .to_string()
                .contains("transport-assisted persisted requests remain unsupported"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected transport-assisted request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected transport-assisted request must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_basenames_transport_assisted_direct_path() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = basenames_transport_direct_request();
        seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

        let persisted = persist_basenames_exact_name_verified_resolution_transport_direct(
            database.pool(),
            &request,
        )
        .await?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .expect("execution trace must exist after Basenames transport persistence");
        assert_eq!(loaded_trace, request.trace);

        let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .expect("execution outcome must exist after Basenames transport persistence");
        assert_eq!(loaded_outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_basenames_transport_direct_with_one_pool_connection() -> Result<()> {
        let database = TestDatabase::new_with_max_connections(1).await?;
        let request = basenames_transport_direct_request();
        seed_supported_resolution_storage(&database, &request).await?;

        let persisted = timeout(
            Duration::from_secs(5),
            persist_basenames_exact_name_verified_resolution_transport_direct(
                database.pool(),
                &request,
            ),
        )
        .await
        .expect(
            "Basenames transport-direct persistence should not block waiting for a second pool checkout",
        )?;
        assert_eq!(
            persisted,
            PersistedVerifiedResolutionIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );
        assert_eq!(
            load_execution_trace(database.pool(), persisted.execution_trace_id).await?,
            Some(request.trace.clone())
        );
        assert_eq!(
            load_execution_outcome(database.pool(), &persisted.cache_key).await?,
            Some(request.outcome.clone())
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_basenames_transport_direct_with_wrong_transport_contract() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = basenames_transport_direct_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000032);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.request_metadata["transport"]["contract_address"] =
            json!("0x0000000000000000000000000000000000000bad");

        let error = persist_basenames_exact_name_verified_resolution_transport_direct(
            database.pool(),
            &request,
        )
        .await
        .expect_err("Basenames transport-direct persistence must reject out-of-class transport");
        assert!(
            error
                .to_string()
                .contains("must use transport base-mainnet -> ethereum-mainnet"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected Basenames transport request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected Basenames transport request must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_basenames_transport_direct_without_ethereum_position() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = basenames_transport_direct_request();
        request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000035);
        request.outcome.execution_trace_id = request.trace.execution_trace_id;
        request.trace.chain_context = json!({
            "requested_positions": [{
                "chain_id": BASE_MAINNET_CHAIN_ID,
                "block_number": 31_000_000,
                "block_hash": "0xbase123"
            }]
        });
        request.outcome.cache_key.requested_chain_positions =
            request.trace.chain_context["requested_positions"].clone();

        let error = persist_basenames_exact_name_verified_resolution_transport_direct(
            database.pool(),
            &request,
        )
        .await
        .expect_err("Basenames transport-direct persistence must require Ethereum position");
        assert!(
            error
                .to_string()
                .contains("must include exactly two chain positions")
                || error
                    .to_string()
                    .contains("must include chain_id ethereum-mainnet"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected Basenames request missing Ethereum position must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected Basenames request missing Ethereum position must not persist outcome rows"
        );

        database.cleanup().await
    }

    fn primary_name_anchor_row_for_namespace(
        namespace: &str,
        address: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
    ) -> PrimaryNameCurrentRow {
        PrimaryNameCurrentRow {
            address: address.to_ascii_lowercase(),
            namespace: namespace.to_owned(),
            coin_type: coin_type.to_owned(),
            claim_status,
            raw_claim_name: (claim_status == PrimaryNameClaimStatus::InvalidName)
                .then(|| "bad name".to_owned()),
            claim_provenance: match namespace {
                ENS_NAMESPACE => json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                    "emitting_address": "0x00000000000000000000000000000000000000ad"
                }),
                BASENAMES_NAMESPACE => json!({
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000124",
                    "emitting_address": "0x00000000000000000000000000000000000000ae"
                }),
                other => panic!("unsupported primary-name test namespace {other}"),
            },
        }
    }

    async fn insert_primary_name_anchor(
        database: &TestDatabase,
        address: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
    ) -> Result<()> {
        upsert_primary_name_current_rows(
            database.pool(),
            &[primary_name_anchor_row_for_namespace(
                ENS_NAMESPACE,
                address,
                coin_type,
                claim_status,
            )],
        )
        .await?;
        Ok(())
    }

    async fn insert_basenames_primary_name_anchor(
        database: &TestDatabase,
        address: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
    ) -> Result<()> {
        upsert_primary_name_current_rows(
            database.pool(),
            &[primary_name_anchor_row_for_namespace(
                BASENAMES_NAMESPACE,
                address,
                coin_type,
                claim_status,
            )],
        )
        .await?;
        Ok(())
    }

    fn verified_primary_name_ref_for_namespace(namespace: &str, name: &str) -> Value {
        json!({
            "logical_name_id": format!("{namespace}:{name}"),
            "namespace": namespace,
            "normalized_name": name,
            "canonical_display_name": name,
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        })
    }

    fn verified_primary_name_ref(name: &str) -> Value {
        verified_primary_name_ref_for_namespace(ENS_NAMESPACE, name)
    }

    fn verified_primary_request_for_namespace(
        namespace: &str,
        execution_trace_id: Uuid,
        normalized_address: &str,
        coin_type: &str,
        verified_primary_name: Value,
    ) -> PersistEnsVerifiedPrimaryNameRequest {
        let request_key =
            normalized_verified_primary_name_request_key(namespace, normalized_address, coin_type);
        let finished_at = timestamp(1_717_172_100);
        let manifest_versions = match namespace {
            ENS_NAMESPACE => manifest_versions(),
            BASENAMES_NAMESPACE => basenames_manifest_versions(),
            other => panic!("unsupported verified-primary test namespace {other}"),
        };
        let contracts_called = match namespace {
            ENS_NAMESPACE => json!([
                {
                    "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                    "selector": "0x9061b923"
                }
            ]),
            BASENAMES_NAMESPACE => json!([
                {
                    "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                    "selector": "0x9061b923"
                }
            ]),
            other => panic!("unsupported verified-primary test namespace {other}"),
        };
        let gateway_digests = match namespace {
            ENS_NAMESPACE => json!([]),
            BASENAMES_NAMESPACE => json!(["sha256:basenames-verified-primary"]),
            other => panic!("unsupported verified-primary test namespace {other}"),
        };
        let steps = match namespace {
            ENS_NAMESPACE => vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_primary_name_claim".to_owned(),
                    input_digest: Some("sha256:claim-input".to_owned()),
                    output_digest: Some("sha256:claim-output".to_owned()),
                    latency_ms: Some(2),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "normalize_claimed_name".to_owned(),
                    input_digest: Some("sha256:normalize-input".to_owned()),
                    output_digest: Some("sha256:normalize-output".to_owned()),
                    latency_ms: Some(1),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "normalizer_version": "uts46-v1"
                    }),
                },
                ExecutionTraceStep {
                    step_index: 2,
                    step_kind: "call_universal_resolver".to_owned(),
                    input_digest: Some("sha256:resolver-input".to_owned()),
                    output_digest: Some("sha256:resolver-output".to_owned()),
                    latency_ms: Some(14),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "name": "alice.eth",
                        "coin_type": coin_type
                    }),
                },
            ],
            BASENAMES_NAMESPACE => vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_primary_name_claim".to_owned(),
                    input_digest: Some("sha256:claim-input".to_owned()),
                    output_digest: Some("sha256:claim-output".to_owned()),
                    latency_ms: Some(2),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "call_l1_resolver".to_owned(),
                    input_digest: Some("sha256:l1-input".to_owned()),
                    output_digest: Some("sha256:l1-output".to_owned()),
                    latency_ms: Some(21),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type
                    }),
                },
                ExecutionTraceStep {
                    step_index: 2,
                    step_kind: "complete_offchain_lookup".to_owned(),
                    input_digest: Some("sha256:gateway-input".to_owned()),
                    output_digest: Some("sha256:gateway-output".to_owned()),
                    latency_ms: Some(33),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "gateway": "https://basenames.example.test"
                    }),
                },
            ],
            other => panic!("unsupported verified-primary test namespace {other}"),
        };
        PersistEnsVerifiedPrimaryNameRequest {
            trace: ExecutionTrace {
                execution_trace_id,
                request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                request_key: request_key.clone(),
                namespace: namespace.to_owned(),
                chain_context: json!({
                    "requested_positions": requested_chain_positions(),
                    "topology_version_boundary": {
                        ETHEREUM_MAINNET_CHAIN_ID: 21_000_000
                    }
                }),
                manifest_context: json!({
                    "manifest_versions": manifest_versions.clone(),
                    "rollout_boundary": "shadow"
                }),
                contracts_called,
                gateway_digests,
                final_payload: Some(json!({
                    "verified_primary_name": verified_primary_name.clone()
                })),
                failure_payload: None,
                request_metadata: json!({
                    "normalized_address": normalized_address,
                    "coin_type": coin_type,
                    "namespace": namespace
                }),
                finished_at: Some(finished_at),
                steps,
            },
            outcome: ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key,
                    requested_chain_positions: requested_chain_positions(),
                    manifest_versions,
                    topology_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bbb1,
                    )),
                    record_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bbb2,
                    )),
                },
                execution_trace_id,
                request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                namespace: namespace.to_owned(),
                outcome_payload: Some(json!({
                    "verified_primary_name": verified_primary_name
                })),
                failure_payload: None,
                finished_at,
            },
        }
    }

    fn verified_primary_request(
        execution_trace_id: Uuid,
        normalized_address: &str,
        coin_type: &str,
        verified_primary_name: Value,
    ) -> PersistEnsVerifiedPrimaryNameRequest {
        verified_primary_request_for_namespace(
            ENS_NAMESPACE,
            execution_trace_id,
            normalized_address,
            coin_type,
            verified_primary_name,
        )
    }

    fn expected_verified_primary_readback_provenance(
        request: &PersistEnsVerifiedPrimaryNameRequest,
    ) -> VerifiedPrimaryNameReadbackProvenance {
        VerifiedPrimaryNameReadbackProvenance {
            execution_trace_id: request.trace.execution_trace_id,
            manifest_versions: request.outcome.cache_key.manifest_versions.clone(),
        }
    }

    fn verified_primary_success_request() -> PersistEnsVerifiedPrimaryNameRequest {
        verified_primary_request(
            Uuid::from_u128(0x0e7ec7ace00000000000000000000021),
            "0x00000000000000000000000000000000000000aa",
            "60",
            json!({
                "status": "success",
                "name": verified_primary_name_ref("alice.eth")
            }),
        )
    }

    fn verified_primary_mismatch_request() -> PersistEnsVerifiedPrimaryNameRequest {
        verified_primary_request(
            Uuid::from_u128(0x0e7ec7ace00000000000000000000022),
            "0x00000000000000000000000000000000000000ab",
            "60",
            json!({
                "status": "mismatch",
                "name": verified_primary_name_ref("alice.eth"),
                "failure_reason": "resolved_target_mismatch"
            }),
        )
    }

    fn verified_primary_not_found_request() -> PersistEnsVerifiedPrimaryNameRequest {
        let mut request = verified_primary_request(
            Uuid::from_u128(0x0e7ec7ace00000000000000000000023),
            "0x00000000000000000000000000000000000000ac",
            "60",
            json!({
                "status": "not_found"
            }),
        );
        request.trace.contracts_called = json!([]);
        request.trace.steps = vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_primary_name_claim".to_owned(),
            input_digest: Some("sha256:claim-input".to_owned()),
            output_digest: Some("sha256:claim-output".to_owned()),
            latency_ms: Some(2),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "address": "0x00000000000000000000000000000000000000ac",
                "coin_type": "60"
            }),
        }];
        request
    }

    fn verified_primary_invalid_name_request() -> PersistEnsVerifiedPrimaryNameRequest {
        let mut request = verified_primary_request(
            Uuid::from_u128(0x0e7ec7ace00000000000000000000024),
            "0x00000000000000000000000000000000000000ad",
            "60",
            json!({
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable"
            }),
        );
        request.trace.contracts_called = json!([]);
        request.trace.steps = vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "address": "0x00000000000000000000000000000000000000ad",
                    "coin_type": "60"
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "normalize_claimed_name".to_owned(),
                input_digest: Some("sha256:normalize-input".to_owned()),
                output_digest: Some("sha256:normalize-output".to_owned()),
                latency_ms: Some(1),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "normalizer_version": "uts46-v1",
                    "error": "label_has_whitespace"
                }),
            },
        ];
        request
    }

    fn verified_primary_execution_failed_request() -> PersistEnsVerifiedPrimaryNameRequest {
        let mut request = verified_primary_request(
            Uuid::from_u128(0x0e7ec7ace00000000000000000000025),
            "0x00000000000000000000000000000000000000ae",
            "60",
            json!({
                "status": "execution_failed",
                "failure_reason": "resolver_call_reverted"
            }),
        );
        request.trace.final_payload = None;
        request.trace.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "stage": "call_universal_resolver"
        }));
        request.outcome.failure_payload = Some(json!({
            "failure_reason": "resolver_call_reverted",
            "reverted": true
        }));
        request
    }

    fn verified_primary_basenames_success_request() -> PersistEnsVerifiedPrimaryNameRequest {
        verified_primary_request_for_namespace(
            BASENAMES_NAMESPACE,
            Uuid::from_u128(0x0e7ec7ace00000000000000000000027),
            "0x00000000000000000000000000000000000000ba",
            "60",
            json!({
                "status": "success",
                "name": verified_primary_name_ref_for_namespace(
                    BASENAMES_NAMESPACE,
                    "alice.base.eth"
                )
            }),
        )
    }

    fn verified_primary_basenames_not_found_request() -> PersistEnsVerifiedPrimaryNameRequest {
        let mut request = verified_primary_request_for_namespace(
            BASENAMES_NAMESPACE,
            Uuid::from_u128(0x0e7ec7ace00000000000000000000028),
            "0x00000000000000000000000000000000000000bb",
            "60",
            json!({
                "status": "not_found"
            }),
        );
        request.trace.contracts_called = json!([]);
        request.trace.gateway_digests = json!([]);
        request.trace.steps = vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_primary_name_claim".to_owned(),
            input_digest: Some("sha256:claim-input".to_owned()),
            output_digest: Some("sha256:claim-output".to_owned()),
            latency_ms: Some(2),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "address": "0x00000000000000000000000000000000000000bb",
                "coin_type": "60"
            }),
        }];
        request
    }

    fn verified_primary_basenames_invalid_name_request() -> PersistEnsVerifiedPrimaryNameRequest {
        let mut request = verified_primary_request_for_namespace(
            BASENAMES_NAMESPACE,
            Uuid::from_u128(0x0e7ec7ace00000000000000000000029),
            "0x00000000000000000000000000000000000000bc",
            "60",
            json!({
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable"
            }),
        );
        request.trace.contracts_called = json!([]);
        request.trace.gateway_digests = json!([]);
        request.trace.steps = vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "address": "0x00000000000000000000000000000000000000bc",
                    "coin_type": "60"
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "normalize_claimed_name".to_owned(),
                input_digest: Some("sha256:normalize-input".to_owned()),
                output_digest: Some("sha256:normalize-output".to_owned()),
                latency_ms: Some(1),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "normalizer_version": "uts46-v1",
                    "error": "label_has_whitespace"
                }),
            },
        ];
        request
    }

    #[tokio::test]
    async fn persists_verified_primary_success_and_reads_back() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_success_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000aa",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        assert_eq!(
            persisted,
            PersistedVerifiedPrimaryNameIdentity {
                execution_trace_id: request.trace.execution_trace_id,
                cache_key: request.outcome.cache_key.clone(),
            }
        );

        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("verified-primary readback must exist");
        assert_eq!(loaded.execution_trace_id, request.trace.execution_trace_id);
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "success",
                "name": verified_primary_name_ref("alice.eth")
            })
        );
        assert_eq!(loaded.trace, request.trace);
        assert_eq!(loaded.outcome, request.outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_basenames_verified_primary_success_and_reads_back() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_basenames_success_request();
        insert_basenames_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000ba",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("Basenames verified-primary readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "success",
                "name": verified_primary_name_ref_for_namespace(
                    BASENAMES_NAMESPACE,
                    "alice.base.eth"
                )
            })
        );
        assert_eq!(loaded.trace.namespace, BASENAMES_NAMESPACE);
        assert_eq!(loaded.outcome.namespace, BASENAMES_NAMESPACE);

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_basenames_verified_primary_not_found_without_l1_resolver_call() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let request = verified_primary_basenames_not_found_request();
        insert_basenames_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000bb",
            "60",
            PrimaryNameClaimStatus::NotFound,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("Basenames not_found readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({ "status": "not_found" })
        );
        assert_eq!(loaded.trace.contracts_called, json!([]));
        assert_eq!(loaded.trace.gateway_digests, json!([]));

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_basenames_verified_primary_invalid_name_without_l1_resolver_call()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_basenames_invalid_name_request();
        insert_basenames_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000bc",
            "60",
            PrimaryNameClaimStatus::InvalidName,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("Basenames invalid_name readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable"
            })
        );
        assert_eq!(loaded.trace.contracts_called, json!([]));
        assert_eq!(loaded.trace.gateway_digests, json!([]));

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_verified_primary_mismatch_and_reads_back() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_mismatch_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000ab",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("mismatch readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "mismatch",
                "name": verified_primary_name_ref("alice.eth"),
                "failure_reason": "resolved_target_mismatch"
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_verified_primary_not_found_without_resolver_call() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_not_found_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000ac",
            "60",
            PrimaryNameClaimStatus::NotFound,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("not_found readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({ "status": "not_found" })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_verified_primary_invalid_name_without_resolver_call() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_invalid_name_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000ad",
            "60",
            PrimaryNameClaimStatus::InvalidName,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("invalid_name readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable"
            })
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn persists_verified_primary_execution_failed_with_failure_payloads() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_execution_failed_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000ae",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("execution_failed readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );
        assert_eq!(
            loaded.verified_primary_name,
            json!({
                "status": "execution_failed",
                "failure_reason": "resolver_call_reverted"
            })
        );
        assert_eq!(loaded.trace.failure_payload, request.trace.failure_payload);
        assert_eq!(
            loaded.outcome.failure_payload,
            request.outcome.failure_payload
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn verified_primary_readback_provenance_remains_execution_local() -> Result<()> {
        let database = TestDatabase::new().await?;
        let mut request = verified_primary_success_request();
        let metadata = request
            .trace
            .request_metadata
            .as_object_mut()
            .expect("verified-primary request_metadata must be an object");
        metadata.insert(
            "verified_primary_name_lookup".to_owned(),
            json!({
                "address": "0x00000000000000000000000000000000000000aa",
                "namespace": ENS_NAMESPACE,
                "coin_type": "60",
            }),
        );
        metadata.insert(
            "verified_primary_name_invalidation".to_owned(),
            json!({
                "claim_status": "success",
                "primary_claim_source": {
                    "seed": "claim-side-only"
                },
            }),
        );
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000aa",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
        let loaded =
            load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
                .await?
                .expect("verified-primary readback must exist");
        assert_eq!(
            loaded.provenance,
            expected_verified_primary_readback_provenance(&request)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_verified_primary_without_primary_name_anchor() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_success_request();

        let error = persist_ens_verified_primary_name(database.pool(), &request)
            .await
            .expect_err("missing tuple anchor must be rejected");
        assert!(
            error
                .to_string()
                .contains("requires primary_names_current anchor"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "rejected verified-primary request must not persist trace rows"
        );
        assert!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "rejected verified-primary request must not persist outcome rows"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn verified_primary_readback_returns_none_when_anchor_is_missing() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_success_request();
        upsert_execution_trace(database.pool(), &request.trace).await?;
        upsert_execution_outcome(database.pool(), &request.outcome).await?;

        assert!(
            load_persisted_ens_verified_primary_name(database.pool(), &request.outcome.cache_key)
                .await?
                .is_none(),
            "readback must stay gated on primary_names_current tuple presence"
        );

        database.cleanup().await
    }

    #[test]
    fn rejects_unnormalized_verified_primary_request_key() -> Result<()> {
        let mut request = verified_primary_success_request();
        request.trace.request_key = "ens:0x00000000000000000000000000000000000000AA:60".to_owned();
        request.outcome.cache_key.request_key = request.trace.request_key.clone();

        let error = validate_verified_primary_request(&request)
            .expect_err("unnormalized verified-primary request_key must be rejected");
        assert!(
            error.to_string().contains(
                "does not match expected ens:0x00000000000000000000000000000000000000aa:60"
            ),
            "unexpected error: {error:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rolls_back_verified_primary_trace_when_outcome_write_fails() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = verified_primary_success_request();
        insert_primary_name_anchor(
            &database,
            "0x00000000000000000000000000000000000000aa",
            "60",
            PrimaryNameClaimStatus::Success,
        )
        .await?;

        let mut conflicting_trace = request.trace.clone();
        conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
        conflicting_trace.request_type = VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned();
        conflicting_trace.request_key = "ens:alice.eth:addr:60".to_owned();
        conflicting_trace.final_payload = Some(json!({
            "verified_queries": [{
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            }]
        }));
        conflicting_trace.request_metadata = json!({
            "surface": "alice.eth",
            "record_key": "addr:60",
            "normalizer_version": "uts46-v1"
        });
        upsert_execution_trace(database.pool(), &conflicting_trace).await?;

        let mut conflicting_outcome = request.outcome.clone();
        conflicting_outcome.execution_trace_id = conflicting_trace.execution_trace_id;
        conflicting_outcome.request_type = conflicting_trace.request_type.clone();
        conflicting_outcome.namespace = "basenames".to_owned();
        conflicting_outcome.outcome_payload = Some(json!({
            "verified_queries": [{
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            }]
        }));
        upsert_execution_outcome(database.pool(), &conflicting_outcome).await?;

        let error = persist_ens_verified_primary_name(database.pool(), &request)
            .await
            .expect_err("conflicting cache identity must roll back verified-primary writes");
        assert!(
            error
                .to_string()
                .contains("execution outcome cache identity mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            load_execution_trace(database.pool(), request.trace.execution_trace_id)
                .await?
                .is_none(),
            "failed verified-primary persistence must not leave a trace row behind"
        );
        assert_eq!(
            load_execution_outcome(database.pool(), &request.outcome.cache_key).await?,
            Some(conflicting_outcome),
            "the pre-existing conflicting outcome must remain untouched"
        );

        database.cleanup().await
    }
}
