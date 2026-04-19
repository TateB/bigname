//! ENS verified-resolution exact-surface execution persistence bootstrap.

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, RawCallSnapshot,
    load_primary_name_current, upsert_execution_outcome_in_transaction,
    upsert_execution_trace_in_transaction, upsert_raw_call_snapshots_in_transaction,
};
use serde_json::{Map, Value};
use sqlx::PgPool;
use uuid::Uuid;

#[cfg(test)]
use bigname_storage::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, upsert_execution_outcome,
    upsert_execution_trace, upsert_primary_name_current_rows,
};

pub use bigname_storage::{
    CanonicalityState, ExecutionTraceStep, load_execution_outcome, load_execution_trace,
    load_raw_call_snapshots_by_block_hash,
};

pub const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";
pub const VERIFIED_PRIMARY_NAME_REQUEST_TYPE: &str = "verified_primary_name";
pub const ENS_NAMESPACE: &str = "ens";
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = "ens_execution";
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const ENS_UNIVERSAL_RESOLVER_ADDRESS: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
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
    "ens-verified-resolution-direct-producer-ready"
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

    if !request.raw_call_snapshots.is_empty() {
        upsert_raw_call_snapshots_in_transaction(&mut transaction, &request.raw_call_snapshots)
            .await?;
    }

    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
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

/// Persist one ENS verified-primary result for an exact `{address, namespace, coin_type}` tuple
/// and return the storage identity the route layer can load back.
pub async fn persist_ens_verified_primary_name(
    pool: &PgPool,
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<PersistedVerifiedPrimaryNameIdentity> {
    let validated = validate_verified_primary_request(request)?;
    ensure_primary_name_anchor_exists(pool, &validated.tuple).await?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for ENS verified-primary persistence")?;

    let trace = upsert_execution_trace_in_transaction(&mut transaction, &request.trace).await?;
    let outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &request.outcome).await?;

    if trace.execution_trace_id != outcome.execution_trace_id {
        bail!(
            "persisted ENS verified-primary trace {} does not match outcome trace {}",
            trace.execution_trace_id,
            outcome.execution_trace_id
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "persisted ENS verified-primary request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit ENS verified-primary persistence")?;

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

    let trace = load_execution_trace(pool, outcome.execution_trace_id)
        .await?
        .with_context(|| {
            format!(
                "failed to load persisted ENS verified-primary trace {}",
                outcome.execution_trace_id
            )
        })?;

    let validated = validate_verified_primary_trace_and_outcome(&trace, &outcome)?;
    let provenance = extract_verified_primary_readback_provenance(&trace, &outcome)?;
    if load_primary_name_current(
        pool,
        &validated.tuple.normalized_address,
        ENS_NAMESPACE,
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
enum SupportedVerifiedRecordKey {
    Addr { coin_type: String },
    Avatar,
    Contenthash,
    Text,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupportedResolutionPathClass {
    Direct,
    AliasOnly,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SupportedResolutionStepSummary {
    saw_universal_resolver_call: bool,
    saw_alias_step: bool,
}

fn validate_verified_primary_request(
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(&request.trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        request.outcome.outcome_payload.as_ref(),
        "ENS verified-primary outcome_payload",
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
        "ENS verified-primary outcome_payload",
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
    let cache_manifest_versions = required_array(
        Some(&outcome.cache_key.manifest_versions),
        "ENS verified-primary cache_key.manifest_versions",
    )?;
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let trace_manifest_versions = required_array(
            Some(trace_manifest_versions),
            "ENS verified-primary trace.manifest_context.manifest_versions",
        )?;
        if trace_manifest_versions != cache_manifest_versions {
            bail!(
                "ENS verified-primary trace.manifest_context.manifest_versions must match cache_key.manifest_versions"
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

fn extract_verified_primary_tuple(trace: &ExecutionTrace) -> Result<VerifiedPrimaryNameTuple> {
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        "ENS verified-primary trace.request_metadata",
    )?;
    let normalized_address = required_string(
        request_metadata,
        "normalized_address",
        "ENS verified-primary trace.request_metadata",
    )?
    .to_owned();
    if normalized_address != normalize_address(&normalized_address) {
        bail!(
            "ENS verified-primary trace.request_metadata.normalized_address must already be lowercase"
        );
    }

    let coin_type = required_coin_type_field(
        request_metadata,
        "coin_type",
        "ENS verified-primary trace.request_metadata",
    )?;
    if let Some(namespace) = optional_nonempty_string_field(
        request_metadata,
        "namespace",
        "ENS verified-primary trace.request_metadata",
    )? {
        if namespace != ENS_NAMESPACE {
            bail!(
                "ENS verified-primary trace.request_metadata.namespace must be {}",
                ENS_NAMESPACE
            );
        }
    }

    Ok(VerifiedPrimaryNameTuple {
        normalized_address,
        coin_type,
    })
}

fn extract_verified_primary_name_section(
    payload: Option<&Value>,
    context: &str,
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
            "ENS verified-primary only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
        ),
    };

    Ok(VerifiedPrimaryNameSection {
        section: Value::Object(section.clone()),
        status,
    })
}

fn validate_verified_primary_name_ref(value: Option<&Value>, context: &str) -> Result<()> {
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

    if namespace != ENS_NAMESPACE {
        bail!("{context}.namespace must be {ENS_NAMESPACE}");
    }
    if logical_name_id != format!("{ENS_NAMESPACE}:{normalized_name}") {
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
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "ENS verified-primary trace {} must use request_type {}",
            trace.execution_trace_id,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if trace.namespace != ENS_NAMESPACE {
        bail!(
            "ENS verified-primary trace {} must use namespace {}",
            trace.execution_trace_id,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS verified-primary outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let expected_request_key =
        normalized_verified_primary_name_request_key(&tuple.normalized_address, &tuple.coin_type);
    if trace.request_key != expected_request_key {
        bail!(
            "ENS verified-primary trace {} request_key {} does not match expected {}",
            trace.execution_trace_id,
            trace.request_key,
            expected_request_key
        );
    }

    let requested_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS verified-primary trace.chain_context.requested_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS verified-primary trace.chain_context.requested_positions",
    )?;

    let gateway_digests = required_array(
        Some(&trace.gateway_digests),
        "ENS verified-primary trace.gateway_digests",
    )?;
    if !gateway_digests.is_empty() {
        bail!("ENS verified-primary must keep gateway_digests empty");
    }

    if !manifest_versions_include_source_family_for_context(
        Some(&trace.manifest_context),
        Some(&outcome.cache_key.manifest_versions),
        ENS_EXECUTION_SOURCE_FAMILY,
        "ENS verified-primary",
    )? {
        bail!(
            "ENS verified-primary must include source_family {} in manifest context or cache key",
            ENS_EXECUTION_SOURCE_FAMILY
        );
    }

    let step_summary = ensure_steps_do_not_use_deferred_execution_paths(
        &trace.steps,
        trace.execution_trace_id,
        "ENS verified-primary",
    )?;
    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        if !step_summary.saw_universal_resolver_call {
            bail!(
                "ENS verified-primary trace {} must include step_kind call_universal_resolver for status {:?}",
                trace.execution_trace_id,
                verified_primary_name.status
            );
        }
        ensure_contains_universal_resolver_call(
            &trace.contracts_called,
            trace.execution_trace_id,
            "ENS verified-primary",
        )?;
    } else if !required_array(
        Some(&trace.contracts_called),
        "ENS verified-primary trace.contracts_called",
    )?
    .is_empty()
    {
        ensure_contains_universal_resolver_call(
            &trace.contracts_called,
            trace.execution_trace_id,
            "ENS verified-primary",
        )?;
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
    if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE {
        bail!(
            "ENS verified-primary outcome for request_key {} must use request_type {}",
            outcome.cache_key.request_key,
            VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        );
    }
    if outcome.namespace != ENS_NAMESPACE {
        bail!(
            "ENS verified-primary outcome for request_key {} must use namespace {}",
            outcome.cache_key.request_key,
            ENS_NAMESPACE
        );
    }
    if outcome.execution_trace_id != trace.execution_trace_id {
        bail!(
            "ENS verified-primary outcome trace {} does not match trace {}",
            outcome.execution_trace_id,
            trace.execution_trace_id
        );
    }

    let trace_finished_at = trace.finished_at.with_context(|| {
        format!(
            "ENS verified-primary trace {} must set finished_at",
            trace.execution_trace_id
        )
    })?;
    if outcome.finished_at != trace_finished_at {
        bail!(
            "ENS verified-primary outcome finished_at {} does not match trace finished_at {}",
            outcome.finished_at,
            trace_finished_at
        );
    }

    let expected_request_key =
        normalized_verified_primary_name_request_key(&tuple.normalized_address, &tuple.coin_type);
    if outcome.cache_key.request_key != expected_request_key {
        bail!(
            "ENS verified-primary outcome request_key {} does not match expected {}",
            outcome.cache_key.request_key,
            expected_request_key
        );
    }
    if outcome.cache_key.request_key != trace.request_key {
        bail!(
            "ENS verified-primary outcome request_key {} does not match trace request_key {}",
            outcome.cache_key.request_key,
            trace.request_key
        );
    }

    let requested_positions = required_chain_positions(
        Some(&outcome.cache_key.requested_chain_positions),
        "ENS verified-primary cache_key.requested_chain_positions",
    )?;
    ensure_single_ethereum_mainnet_position(
        &requested_positions,
        "ENS verified-primary cache_key.requested_chain_positions",
    )?;

    let trace_positions = required_chain_positions(
        trace.chain_context.get("requested_positions"),
        "ENS verified-primary trace.chain_context.requested_positions",
    )?;
    if trace_positions != requested_positions {
        bail!(
            "ENS verified-primary trace.chain_context.requested_positions must match cache_key.requested_chain_positions"
        );
    }

    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            required_object(
                outcome.failure_payload.as_ref(),
                "ENS verified-primary execution_failed outcome.failure_payload",
            )?;
        }
        _ if outcome.failure_payload.is_some() => {
            bail!(
                "ENS verified-primary outcome for request_key {} must not set failure_payload unless status is execution_failed",
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
    match verified_primary_name.status {
        VerifiedPrimaryNameStatus::ExecutionFailed => {
            if trace.final_payload.is_some() {
                bail!(
                    "ENS verified-primary execution_failed trace {} must not set final_payload",
                    trace.execution_trace_id
                );
            }
            required_object(
                trace.failure_payload.as_ref(),
                "ENS verified-primary execution_failed trace.failure_payload",
            )?;
        }
        _ => {
            if trace.failure_payload.is_some() {
                bail!(
                    "ENS verified-primary trace {} must not set failure_payload unless status is execution_failed",
                    trace.execution_trace_id
                );
            }
            let final_payload = trace.final_payload.as_ref().with_context(|| {
                format!(
                    "ENS verified-primary trace {} must set final_payload when status is not execution_failed",
                    trace.execution_trace_id
                )
            })?;
            let final_verified_primary_name = extract_verified_primary_name_section(
                Some(final_payload),
                "ENS verified-primary trace.final_payload",
            )?;
            if final_verified_primary_name != *verified_primary_name {
                bail!(
                    "ENS verified-primary trace.final_payload.verified_primary_name must match outcome_payload.verified_primary_name"
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
    if load_primary_name_current(
        pool,
        &tuple.normalized_address,
        ENS_NAMESPACE,
        &tuple.coin_type,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }

    bail!(
        "ENS verified-primary persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
        tuple.normalized_address,
        ENS_NAMESPACE,
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
    if let Some(coin_type) = record_key.strip_prefix("addr:") {
        if !coin_type.is_empty() && coin_type.as_bytes().iter().all(u8::is_ascii_digit) {
            return Ok(SupportedVerifiedRecordKey::Addr {
                coin_type: coin_type.to_owned(),
            });
        }
    }

    if record_key == "contenthash" {
        return Ok(SupportedVerifiedRecordKey::Contenthash);
    }

    if record_key == "avatar" {
        return Ok(SupportedVerifiedRecordKey::Avatar);
    }

    if let Some(text_key) = record_key.strip_prefix("text:") {
        if !text_key.is_empty() {
            return Ok(SupportedVerifiedRecordKey::Text);
        }
    }

    bail!(
        "ENS direct-path verified resolution only supports addr:<coin_type>, avatar, contenthash, and text:<key> selectors, found {}",
        record_key
    );
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

fn normalized_request_key(surface: &str, ordered_record_keys: &[String]) -> String {
    let mut normalized_record_keys = ordered_record_keys.to_vec();
    normalized_record_keys.sort_unstable();
    format!(
        "{ENS_NAMESPACE}:{surface}:{}",
        normalized_record_keys.join(",")
    )
}

fn normalized_verified_primary_name_request_key(
    normalized_address: &str,
    coin_type: &str,
) -> String {
    format!(
        "{ENS_NAMESPACE}:{}:{coin_type}",
        normalize_address(normalized_address)
    )
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
    if path_class == SupportedResolutionPathClass::Direct && step_summary.saw_alias_step {
        bail!(
            "ENS direct-path verified resolution trace {} must not persist alias steps without binding_kind {}",
            execution_trace_id,
            RESOLVER_ALIAS_PATH_BINDING_KIND
        );
    }

    Ok(())
}

fn ensure_steps_do_not_use_deferred_execution_paths(
    steps: &[bigname_storage::ExecutionTraceStep],
    execution_trace_id: Uuid,
    context: &str,
) -> Result<SupportedResolutionStepSummary> {
    let mut summary = SupportedResolutionStepSummary::default();
    for step in steps {
        let normalized = step.step_kind.to_ascii_lowercase();
        if normalized.contains("wildcard")
            || normalized.contains("ccip")
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
        Some(LINKED_SUBREGISTRY_PATH_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist non-alias ancestor-selected binding_kind {}",
            execution_trace_id,
            LINKED_SUBREGISTRY_PATH_BINDING_KIND
        ),
        Some(OBSERVED_WILDCARD_PATH_BINDING_KIND) => bail!(
            "ENS direct-path verified resolution trace {} must not persist wildcard-derived binding_kind {}",
            execution_trace_id,
            OBSERVED_WILDCARD_PATH_BINDING_KIND
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
    ensure_wildcard_detail_absent(trace, "ENS direct-path verified resolution")?;
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
        if let Some(name) = payload.get("name").and_then(Value::as_str) {
            if name != surface {
                bail!(
                    "{context} trace {} must anchor call_universal_resolver name {} to request surface {}",
                    execution_trace_id,
                    name,
                    surface
                );
            }
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
        validate_verified_primary_name_ref(Some(hop), &format!("{alias_context}.hops[{index}]"))?;
    }
    if hops.last() != final_target {
        bail!("{alias_context}.hops last element must match final_target");
    }

    Ok(true)
}

fn ensure_wildcard_detail_absent(trace: &ExecutionTrace, context: &str) -> Result<()> {
    let Some(wildcard) = persisted_trace_detail_object(trace, "wildcard") else {
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
            )?;
            true
        }
    };
    let matched_labels = required_array(
        wildcard.get("matched_labels"),
        &format!("{wildcard_context}.matched_labels"),
    )?;
    if source_present || !matched_labels.is_empty() {
        bail!(
            "{context} only supports wildcard.source=null with matched_labels=[] for persisted exact-surface requests"
        );
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
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        types::time::OffsetDateTime,
    };

    use super::*;
    use bigname_storage::{MIGRATOR, default_database_url};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
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
                .max_connections(5)
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
        let request_key = normalized_request_key("alice.eth", &ordered_record_keys);
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
        let request_key = normalized_request_key("alice.eth", &ordered_record_keys);
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
        let request_key = normalized_request_key("alice.eth", &ordered_record_keys);
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
        let request_key = normalized_request_key("alice.eth", &["text:com.twitter".to_owned()]);
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
        let request_key = normalized_request_key("alice.eth", &["avatar".to_owned()]);
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
    async fn persists_avatar_success_direct_path_and_reads_back_storage_identity() -> Result<()> {
        let database = TestDatabase::new().await?;
        let request = avatar_success_request();

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
        let request = alias_only_text_request();

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
        let request_key = normalized_request_key("alice.eth", &duplicate_record_keys);
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
        let request_key = normalized_request_key("alice.eth", &ordered_record_keys);
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
        let request_key = normalized_request_key("alice.eth", &ordered_record_keys);
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

    fn primary_name_anchor_row(
        address: &str,
        coin_type: &str,
        claim_status: PrimaryNameClaimStatus,
    ) -> PrimaryNameCurrentRow {
        PrimaryNameCurrentRow {
            address: address.to_ascii_lowercase(),
            namespace: ENS_NAMESPACE.to_owned(),
            coin_type: coin_type.to_owned(),
            claim_status,
            raw_claim_name: (claim_status == PrimaryNameClaimStatus::InvalidName)
                .then(|| "bad name".to_owned()),
            claim_provenance: json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                "emitting_address": "0x00000000000000000000000000000000000000ad"
            }),
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
            &[primary_name_anchor_row(address, coin_type, claim_status)],
        )
        .await?;
        Ok(())
    }

    fn verified_primary_name_ref(name: &str) -> Value {
        json!({
            "logical_name_id": format!("{ENS_NAMESPACE}:{name}"),
            "namespace": ENS_NAMESPACE,
            "normalized_name": name,
            "canonical_display_name": name,
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        })
    }

    fn verified_primary_request(
        execution_trace_id: Uuid,
        normalized_address: &str,
        coin_type: &str,
        verified_primary_name: Value,
    ) -> PersistEnsVerifiedPrimaryNameRequest {
        let request_key =
            normalized_verified_primary_name_request_key(normalized_address, coin_type);
        let finished_at = timestamp(1_717_172_100);
        PersistEnsVerifiedPrimaryNameRequest {
            trace: ExecutionTrace {
                execution_trace_id,
                request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
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
                    "verified_primary_name": verified_primary_name.clone()
                })),
                failure_payload: None,
                request_metadata: json!({
                    "normalized_address": normalized_address,
                    "coin_type": coin_type,
                    "namespace": ENS_NAMESPACE
                }),
                finished_at: Some(finished_at),
                steps: vec![
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
            },
            outcome: ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key,
                    requested_chain_positions: requested_chain_positions(),
                    manifest_versions: manifest_versions(),
                    topology_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bbb1,
                    )),
                    record_version_boundary: version_boundary(Uuid::from_u128(
                        0x0e7ec7ace0000000000000000000bbb2,
                    )),
                },
                execution_trace_id,
                request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                outcome_payload: Some(json!({
                    "verified_primary_name": verified_primary_name
                })),
                failure_payload: None,
                finished_at,
            },
        }
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
