use anyhow::{Context, Result};
use bigname_storage::{
    ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, ExecutionTraceStep, NameCurrentRow,
    RecordInventoryCurrentRow, VerifiedResolutionPathClass, VerifiedResolutionRecord,
    build_resolution_execution_cache_key,
};
use futures_util::future::join_all;
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::ens_resolution_abi::{
    UNIVERSAL_RESOLVER_RESOLVE_SELECTOR, digest_json, dns_encode_name, namehash, selector_hex,
};
use crate::ens_resolution_call::{SelectorCall, execute_record_call};
use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;
use crate::rpc::{ChainRpcUrls, JsonRpcHttpClient};
use crate::{
    ENS_EXECUTION_SOURCE_FAMILY, ENS_NAMESPACE, ENS_UNIVERSAL_RESOLVER_ADDRESS,
    ENS_UNIVERSAL_RESOLVER_ROLE, ETHEREUM_MAINNET_CHAIN_ID, VERIFIED_RESOLUTION_REQUEST_TYPE,
    persist_ens_exact_name_verified_resolution_direct,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsResolutionRecord {
    pub record_key: String,
    pub record_family: String,
    pub selector_key: Option<String>,
}

impl EnsResolutionRecord {
    pub fn new(
        record_key: impl Into<String>,
        record_family: impl Into<String>,
        selector_key: Option<String>,
    ) -> Self {
        Self {
            record_key: record_key.into(),
            record_family: record_family.into(),
            selector_key,
        }
    }
}

impl VerifiedResolutionRecord for EnsResolutionRecord {
    fn record_key(&self) -> &str {
        &self.record_key
    }

    fn record_family(&self) -> &str {
        &self.record_family
    }

    fn selector_key(&self) -> Option<&str> {
        self.selector_key.as_deref()
    }
}

pub struct OnDemandEnsResolutionRequest<'a> {
    pub row: &'a NameCurrentRow,
    pub records: &'a [EnsResolutionRecord],
    pub record_inventory_row: Option<&'a RecordInventoryCurrentRow>,
    pub chain_positions: Value,
    pub chain_rpc_urls: &'a ChainRpcUrls,
    pub use_latest_block_tag: bool,
    pub persist_execution: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnDemandEnsResolutionErrorKind {
    Configuration,
    Unsupported,
    Persistence,
}

#[derive(Debug)]
pub struct OnDemandEnsResolutionError {
    kind: OnDemandEnsResolutionErrorKind,
    message: String,
}

impl OnDemandEnsResolutionError {
    fn configuration(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsResolutionErrorKind::Configuration,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsResolutionErrorKind::Unsupported,
            message: message.into(),
        }
    }

    fn persistence(message: impl Into<String>) -> Self {
        Self {
            kind: OnDemandEnsResolutionErrorKind::Persistence,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> OnDemandEnsResolutionErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for OnDemandEnsResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for OnDemandEnsResolutionError {}

pub async fn execute_ens_universal_resolver_verified_resolution(
    pool: &PgPool,
    request: OnDemandEnsResolutionRequest<'_>,
) -> std::result::Result<ExecutionOutcome, OnDemandEnsResolutionError> {
    let supported_records = bigname_storage::supported_resolution_verified_readback_records(
        request.row,
        request.records,
    );
    if supported_records.is_empty() {
        return Err(OnDemandEnsResolutionError::unsupported(
            "on-demand ENS verified resolution has no supported selectors",
        ));
    }
    ensure_on_demand_supported_boundary(request.row, request.record_inventory_row)?;

    let cache_key = build_resolution_execution_cache_key(
        request.row,
        &supported_records,
        request.record_inventory_row,
        request.chain_positions.clone(),
    )
    .map_err(|error| {
        OnDemandEnsResolutionError::persistence(format!(
            "failed to derive on-demand ENS verified resolution cache key: {error}"
        ))
    })?;
    let block = ethereum_mainnet_block(&cache_key).map_err(|error| {
        OnDemandEnsResolutionError::persistence(format!(
            "failed to select Ethereum Mainnet execution block: {error}"
        ))
    })?;
    let Some(provider_url) = request.chain_rpc_urls.url_for(ETHEREUM_MAINNET_CHAIN_ID) else {
        return Err(OnDemandEnsResolutionError::configuration(format!(
            "verified resolution RPC provider for {ETHEREUM_MAINNET_CHAIN_ID} is not configured; set BIGNAME_API_CHAIN_RPC_URLS={ETHEREUM_MAINNET_CHAIN_ID}=<url>"
        )));
    };
    let rpc = JsonRpcHttpClient::new(provider_url).map_err(|error| {
        OnDemandEnsResolutionError::configuration(format!(
            "verified resolution RPC provider for {ETHEREUM_MAINNET_CHAIN_ID} is invalid: {error}"
        ))
    })?;

    let built = build_on_demand_request(
        request.row,
        &supported_records,
        cache_key,
        block,
        &rpc,
        request.use_latest_block_tag,
    )
    .await
    .map_err(|error| {
        OnDemandEnsResolutionError::configuration(format!(
            "failed to execute on-demand ENS verified resolution RPC call: {error}"
        ))
    })?;

    if request.persist_execution {
        persist_ens_exact_name_verified_resolution_direct(pool, &built)
            .await
            .map_err(|error| {
                OnDemandEnsResolutionError::persistence(format!(
                    "failed to persist on-demand ENS verified resolution execution result: {error}"
                ))
            })?;
    }

    Ok(built.outcome)
}

fn ensure_on_demand_supported_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> std::result::Result<(), OnDemandEnsResolutionError> {
    if row.namespace != ENS_NAMESPACE {
        return Err(OnDemandEnsResolutionError::unsupported(format!(
            "on-demand Universal Resolver execution supports namespace {ENS_NAMESPACE}, found {}",
            row.namespace
        )));
    }
    let Some(boundary) =
        bigname_storage::resolution_verified_support_boundary(row, record_inventory_row)
    else {
        return Err(OnDemandEnsResolutionError::unsupported(
            "on-demand ENS verified resolution requires a supported execution path",
        ));
    };
    match boundary.path_class {
        VerifiedResolutionPathClass::Direct
        | VerifiedResolutionPathClass::AliasOnly
        | VerifiedResolutionPathClass::WildcardDerived => Ok(()),
        VerifiedResolutionPathClass::BasenamesTransportDirect => {
            Err(OnDemandEnsResolutionError::unsupported(
                "on-demand ENS verified resolution does not execute Basenames transport paths",
            ))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExecutionBlock {
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
}

async fn build_on_demand_request(
    row: &NameCurrentRow,
    records: &[EnsResolutionRecord],
    cache_key: ExecutionCacheKey,
    block: ExecutionBlock,
    rpc: &JsonRpcHttpClient,
    use_latest_block_tag: bool,
) -> Result<PersistEnsExactNameVerifiedResolutionRequest> {
    let execution_trace_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let finished_at = OffsetDateTime::from_unix_timestamp(now.unix_timestamp())
        .context("failed to normalize on-demand execution timestamp")?;
    let dns_name = dns_encode_name(&row.normalized_name)?;
    let node = namehash(&row.normalized_name)?;
    let mut raw_call_snapshots = Vec::new();
    let mut calls = Vec::new();
    let mut gateway_digests = Vec::new();
    let mut verified_queries = Vec::new();
    let mut steps = vec![declared_topology_step(row, &cache_key, &block)];

    let selector_calls = join_all(records.iter().map(|record| {
        execute_record_call(
            row,
            record,
            &dns_name,
            node,
            &block,
            rpc,
            use_latest_block_tag,
        )
    }))
    .await;

    for (record, selector_call) in records.iter().zip(selector_calls) {
        let selector_call = selector_call?;
        steps.push(call_step(
            steps.len() as i64,
            row,
            record,
            &selector_call,
            &block,
        ));
        verified_queries.push(selector_call.verified_query(execution_trace_id));
        if let Some(summary) = &selector_call.ccip_summary {
            gateway_digests.extend(summary.gateway_digests.iter().cloned());
        }
        raw_call_snapshots.extend(selector_call.raw_call_snapshot);
        calls.push(selector_call.contract_call);
    }

    let all_execution_failed = verified_queries.iter().all(|query| {
        query
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "execution_failed")
    });
    let failure_payload = all_execution_failed.then(|| {
        json!({
            "failure_reason": "resolver_call_failed",
            "stage": "call_universal_resolver",
        })
    });
    let final_payload = (!all_execution_failed).then(|| {
        json!({
            "verified_queries": verified_queries.clone(),
        })
    });
    let outcome_failure_payload = all_execution_failed.then(|| {
        json!({
            "failure_reason": "resolver_call_failed",
            "selector_count": verified_queries.len(),
        })
    });
    let outcome_payload = json!({
        "verified_queries": verified_queries,
    });

    let trace = ExecutionTrace {
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        request_key: cache_key.request_key.clone(),
        namespace: ENS_NAMESPACE.to_owned(),
        chain_context: json!({
            "requested_positions": cache_key.requested_chain_positions.clone(),
        }),
        manifest_context: manifest_context(row),
        contracts_called: Value::Array(calls),
        gateway_digests: json!(gateway_digests),
        final_payload,
        failure_payload,
        request_metadata: request_metadata(row, records),
        finished_at: Some(finished_at),
        steps,
    };
    let outcome = ExecutionOutcome {
        cache_key,
        execution_trace_id,
        request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        outcome_payload: Some(outcome_payload),
        failure_payload: outcome_failure_payload,
        finished_at,
    };

    Ok(PersistEnsExactNameVerifiedResolutionRequest {
        raw_call_snapshots,
        trace,
        outcome,
    })
}

fn ethereum_mainnet_block(cache_key: &ExecutionCacheKey) -> Result<ExecutionBlock> {
    let positions = cache_key
        .requested_chain_positions
        .as_array()
        .context("requested_chain_positions must be an array")?;
    let position = positions
        .iter()
        .find(|position| {
            position
                .get("chain_id")
                .and_then(Value::as_str)
                .is_some_and(|chain_id| chain_id == ETHEREUM_MAINNET_CHAIN_ID)
        })
        .context("requested_chain_positions must include ethereum-mainnet")?;
    Ok(ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: position
            .get("block_number")
            .and_then(Value::as_i64)
            .context("ethereum-mainnet position must include block_number")?,
        block_hash: position
            .get("block_hash")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("ethereum-mainnet position must include block_hash")?
            .to_owned(),
    })
}

fn declared_topology_step(
    row: &NameCurrentRow,
    cache_key: &ExecutionCacheKey,
    block: &ExecutionBlock,
) -> ExecutionTraceStep {
    let payload = json!({
        "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
        "resolver": declared_resolver_address(row),
        "topology_version_boundary": cache_key.topology_version_boundary,
        "record_version_boundary": cache_key.record_version_boundary,
    });
    ExecutionTraceStep {
        step_index: 0,
        step_kind: "load_declared_topology".to_owned(),
        input_digest: Some(digest_json(&row.declared_summary)),
        output_digest: Some(digest_json(&payload)),
        latency_ms: None,
        canonicality_dependency: canonicality_dependency(block),
        step_payload: payload,
    }
}

fn call_step(
    step_index: i64,
    row: &NameCurrentRow,
    record: &EnsResolutionRecord,
    call: &SelectorCall,
    block: &ExecutionBlock,
) -> ExecutionTraceStep {
    let mut payload = json!({
        "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
        "resolver": declared_resolver_address(row),
        "name": row.normalized_name,
        "record_key": record.record_key,
        "selector": selector_hex(UNIVERSAL_RESOLVER_RESOLVE_SELECTOR),
        "resolver_selector": call.resolver_selector,
        "block_selector": call.block_selector.clone(),
        "calldata": call.universal_calldata,
    });
    if let Some(summary) = &call.ccip_summary {
        payload["ccip_read"] = json!({
            "gateway_count": summary.gateway_digests.len(),
            "steps": summary.step_payloads.clone(),
        });
    }
    ExecutionTraceStep {
        step_index,
        step_kind: "call_universal_resolver".to_owned(),
        input_digest: call.request_hash.clone(),
        output_digest: call.response_hash.clone(),
        latency_ms: None,
        canonicality_dependency: canonicality_dependency(block),
        step_payload: payload,
    }
}

fn canonicality_dependency(block: &ExecutionBlock) -> Value {
    json!({
        ETHEREUM_MAINNET_CHAIN_ID: {
            "block_hash": block.block_hash,
            "block_number": block.block_number,
            "state": "canonical",
        }
    })
}

fn request_metadata(row: &NameCurrentRow, records: &[EnsResolutionRecord]) -> Value {
    let mut metadata = json!({
        "surface": row.normalized_name,
        "record_keys": records
            .iter()
            .map(|record| record.record_key.clone())
            .collect::<Vec<_>>(),
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
        "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
    });
    if let Some(binding_kind) = row.binding_kind {
        metadata["binding_kind"] = json!(binding_kind.as_str());
    }
    if let Some(topology) = bigname_storage::projected_resolution_topology(&row.declared_summary) {
        for key in ["alias", "wildcard", "transport"] {
            if let Some(value) = topology.get(key) {
                metadata[key] = value.clone();
            }
        }
    }
    metadata
}

fn manifest_context(row: &NameCurrentRow) -> Value {
    let mut manifest_versions = row
        .provenance
        .get("manifest_versions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !manifest_versions.iter().any(|item| {
        item.get("source_family")
            .and_then(Value::as_str)
            .is_some_and(|source_family| source_family == ENS_EXECUTION_SOURCE_FAMILY)
    }) {
        manifest_versions.push(json!({
            "source_family": ENS_EXECUTION_SOURCE_FAMILY,
            "manifest_version": 1,
            "chain": ETHEREUM_MAINNET_CHAIN_ID,
            "deployment_epoch": "ens_v1",
        }));
    }
    json!({ "manifest_versions": manifest_versions })
}

fn declared_resolver_address(row: &NameCurrentRow) -> Option<String> {
    row.declared_summary
        .get("resolver")
        .and_then(|resolver| resolver.get("address"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
