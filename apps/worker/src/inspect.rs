use anyhow::{Context, Result};
use bigname_manifests::{
    WatchedChainPlan, WatchedContract, WatchedContractChainSummary, WatchedContractSource,
    WatchedContractSummary, load_watched_contracts, plan_watched_contracts,
    summarize_watched_contracts,
};
use bigname_storage::{
    BackfillJob, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange, CanonicalityInspection,
    CanonicalityInspectionStatus, CanonicalityState, DatabaseConfig, ExecutionTraceInspection,
    ExecutionTraceStep, ManifestDriftAlertInspection, ManifestDriftAlertObservation,
    RawFactAuditCounts, StoredLineageRangeBlock,
};
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::time::{OffsetDateTime, UtcOffset};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Args, Debug)]
pub(crate) struct InspectArgs {
    #[command(subcommand)]
    pub(crate) command: InspectCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum InspectCommand {
    #[command(about = "Inspect one persisted backfill job and its child ranges")]
    BackfillJob(InspectBackfillJobArgs),
    #[command(about = "Inspect canonicality and block-scoped audit counts for one block hash")]
    Canonicality(InspectCanonicalityArgs),
    #[command(about = "Inspect one persisted execution trace and its ordered steps")]
    ExecutionTrace(InspectExecutionTraceArgs),
    #[command(about = "Inspect stored manifest drift and proxy implementation alert observations")]
    ManifestDrift(InspectManifestDriftArgs),
    #[command(about = "List stored lineage rows for a bounded chain block range")]
    StoredLineageRange(InspectStoredLineageRangeArgs),
    #[command(about = "Inspect the read-only runtime watch plan derived from active manifests")]
    WatchPlan(InspectWatchPlanArgs),
}

#[derive(Args, Debug)]
pub(crate) struct InspectBackfillJobArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) backfill_job_id: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InspectCanonicalityArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_hash: String,
}

#[derive(Args, Debug)]
pub(crate) struct InspectExecutionTraceArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) execution_trace_id: Uuid,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct InspectManifestDriftArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct InspectStoredLineageRangeArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) range_start_block_number: i64,
    #[arg(long)]
    pub(crate) range_end_block_number: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InspectWatchPlanArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
}

pub(crate) async fn inspect_command(args: InspectArgs) -> Result<()> {
    match args.command {
        InspectCommand::BackfillJob(args) => inspect_backfill_job(args).await,
        InspectCommand::Canonicality(args) => inspect_canonicality(args).await,
        InspectCommand::ExecutionTrace(args) => inspect_execution_trace(args).await,
        InspectCommand::ManifestDrift(args) => inspect_manifest_drift(args).await,
        InspectCommand::StoredLineageRange(args) => inspect_stored_lineage_range(args).await,
        InspectCommand::WatchPlan(args) => inspect_watch_plan(args).await,
    }
}

async fn inspect_backfill_job(args: InspectBackfillJobArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection = load_backfill_job_inspection(&pool, args.backfill_job_id).await?;

    println!("{}", render_backfill_job_inspection(&inspection));
    Ok(())
}

async fn inspect_canonicality(args: InspectCanonicalityArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection =
        bigname_storage::inspect_block_canonicality(&pool, &args.chain_id, &args.block_hash)
            .await?;

    println!("{}", render_canonicality_inspection(&inspection));
    Ok(())
}

async fn inspect_execution_trace(args: InspectExecutionTraceArgs) -> Result<()> {
    let _emit_json = args.json;
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection =
        bigname_storage::load_execution_trace_inspection(&pool, args.execution_trace_id)
            .await?
            .with_context(|| format!("missing execution trace {}", args.execution_trace_id))?;

    println!("{}", render_execution_trace_inspection(&inspection));
    Ok(())
}

async fn inspect_manifest_drift(args: InspectManifestDriftArgs) -> Result<()> {
    let _emit_json = args.json;
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection = bigname_storage::list_manifest_drift_alert_observations(&pool).await?;

    println!("{}", render_manifest_drift_inspection(&inspection));
    Ok(())
}

async fn inspect_stored_lineage_range(args: InspectStoredLineageRangeArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let blocks = bigname_storage::list_stored_lineage_range(
        &pool,
        &args.chain_id,
        args.range_start_block_number,
        args.range_end_block_number,
    )
    .await?;

    println!("{}", render_stored_lineage_range_inspection(&blocks));
    Ok(())
}

async fn inspect_watch_plan(args: InspectWatchPlanArgs) -> Result<()> {
    let _emit_json = args.json;
    let pool = connect_read_only(&args.database).await?;
    let watched_contracts = load_watched_contracts(&pool).await?;
    let summary = summarize_watched_contracts(&watched_contracts);
    let watch_plan = plan_watched_contracts(&watched_contracts);

    println!(
        "{}",
        render_watch_plan_inspection(&watched_contracts, &summary, &watch_plan)
    );
    Ok(())
}

async fn connect_read_only(config: &DatabaseConfig) -> Result<sqlx::PgPool> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| bigname_storage::default_database_url().to_owned());
    let options = PgConnectOptions::from_str(&database_url)
        .context("failed to parse PostgreSQL URL for read-only inspect connection")?
        .options([("default_transaction_read_only", "on")]);

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect_with(options)
        .await
        .context("failed to connect to PostgreSQL for read-only inspect")
}

async fn load_backfill_job_inspection(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
) -> Result<BackfillJobRecord> {
    let job = bigname_storage::load_backfill_job(pool, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    let mut ranges = bigname_storage::load_backfill_ranges(pool, backfill_job_id).await?;
    ranges.sort_by_key(|range| {
        (
            range.range_start_block_number,
            range.range_end_block_number,
            range.backfill_range_id,
        )
    });

    Ok(BackfillJobRecord { job, ranges })
}

fn render_backfill_job_inspection(inspection: &BackfillJobRecord) -> Value {
    json!({
        "job": render_backfill_job(&inspection.job),
        "ranges": inspection
            .ranges
            .iter()
            .map(render_backfill_range)
            .collect::<Vec<_>>(),
    })
}

fn render_backfill_job(job: &BackfillJob) -> Value {
    json!({
        "backfill_job_id": job.backfill_job_id,
        "deployment_profile": job.deployment_profile.as_str(),
        "chain_id": job.chain_id.as_str(),
        "source_identity": job.source_identity.clone(),
        "scan_mode": job.scan_mode.as_str(),
        "status": job.status.as_str(),
        "lifecycle": render_lifecycle_state(job.status),
        "declared_range": render_declared_range(
            job.range_start_block_number,
            job.range_end_block_number,
        ),
        "idempotency_key": job.idempotency_key.as_str(),
        "timestamps": render_timestamps(job.created_at, job.updated_at, job.completed_at),
        "failure": render_failure(job.failure_reason.as_deref(), &job.failure_metadata),
    })
}

fn render_backfill_range(range: &BackfillRange) -> Value {
    json!({
        "backfill_range_id": range.backfill_range_id,
        "backfill_job_id": range.backfill_job_id,
        "status": range.status.as_str(),
        "lifecycle": render_lifecycle_state(range.status),
        "declared_range": render_declared_range(
            range.range_start_block_number,
            range.range_end_block_number,
        ),
        "checkpoint": {
            "block_number": range.checkpoint_block_number,
        },
        "lease": {
            "owner": range.lease_owner.as_deref(),
            "token": range.lease_token.as_deref(),
            "expires_at": range.lease_expires_at.map(format_timestamp),
        },
        "attempt_count": range.attempt_count,
        "timestamps": render_timestamps(range.created_at, range.updated_at, range.completed_at),
        "failure": render_failure(range.failure_reason.as_deref(), &range.failure_metadata),
    })
}

fn render_lifecycle_state(status: BackfillLifecycleStatus) -> Value {
    json!({
        "status": status.as_str(),
        "pending": status == BackfillLifecycleStatus::Pending,
        "reserved": status == BackfillLifecycleStatus::Reserved,
        "running": status == BackfillLifecycleStatus::Running,
        "completed": status == BackfillLifecycleStatus::Completed,
        "failed": status == BackfillLifecycleStatus::Failed,
    })
}

fn render_declared_range(start_block_number: i64, end_block_number: i64) -> Value {
    json!({
        "start_block_number": start_block_number,
        "end_block_number": end_block_number,
    })
}

fn render_timestamps(
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
) -> Value {
    json!({
        "created_at": format_timestamp(created_at),
        "updated_at": format_timestamp(updated_at),
        "completed_at": completed_at.map(format_timestamp),
    })
}

fn render_failure(reason: Option<&str>, metadata: &Value) -> Value {
    json!({
        "reason": reason,
        "metadata": metadata.clone(),
    })
}

fn render_execution_trace_inspection(inspection: &ExecutionTraceInspection) -> Value {
    let trace = &inspection.trace;
    json!({
        "command": "inspect execution-trace",
        "execution_trace_id": trace.execution_trace_id.to_string(),
        "request_type": trace.request_type.as_str(),
        "request_key": trace.request_key.as_str(),
        "namespace": trace.namespace.as_str(),
        "request": {
            "type": trace.request_type.as_str(),
            "key": trace.request_key.as_str(),
            "metadata": trace.request_metadata.clone(),
        },
        "request_metadata": trace.request_metadata.clone(),
        "chain_positions": persisted_context_array(&trace.chain_context, &[
            "chain_positions",
            "requested_positions",
        ]),
        "chain_context": trace.chain_context.clone(),
        "manifest_versions": persisted_context_array(&trace.manifest_context, &[
            "manifest_versions",
            "versions",
        ]),
        "manifest_context": trace.manifest_context.clone(),
        "contracts_called": trace.contracts_called.clone(),
        "gateway_digests": trace.gateway_digests.clone(),
        "status": execution_trace_status(inspection),
        "final_value_digest": persisted_digest_metadata(trace.final_payload.as_ref(), &[
            "final_value_digest",
            "value_digest",
            "digest",
        ]),
        "failure_reason": persisted_failure_reason(trace.failure_payload.as_ref()),
        "finished_at": trace.finished_at.map(format_timestamp),
        "steps": trace
            .steps
            .iter()
            .map(render_execution_trace_step)
            .collect::<Vec<_>>(),
    })
}

fn render_execution_trace_step(step: &ExecutionTraceStep) -> Value {
    json!({
        "step_index": step.step_index,
        "step_kind": step.step_kind.as_str(),
        "input_digest": step.input_digest.as_deref(),
        "output_digest": step.output_digest.as_deref(),
        "latency_ms": step.latency_ms,
        "canonicality_dependency": step.canonicality_dependency.clone(),
        "attachment_digest_metadata": persisted_digest_metadata(Some(&step.step_payload), &[
            "attachment_digest_metadata",
            "attachment_digests",
            "attachments",
        ]),
    })
}

fn render_canonicality_inspection(inspection: &CanonicalityInspection) -> Value {
    json!({
        "chain_id": inspection.chain_id.as_str(),
        "block_hash": inspection.block_hash.as_str(),
        "status": canonicality_inspection_status_label(inspection.status),
        "lineage_canonicality": inspection.lineage_state.map(canonicality_state_label),
        "parent_hash": inspection.parent_hash.as_deref(),
        "block_number": inspection.block_number,
        "raw_fact_counts": render_raw_fact_counts(&inspection.raw_fact_counts),
        "normalized_event_count": inspection.normalized_event_count,
        "states": {
            "observed": inspection.status == CanonicalityInspectionStatus::Observed,
            "canonical": inspection.status == CanonicalityInspectionStatus::Canonical,
            "safe": inspection.status == CanonicalityInspectionStatus::Safe,
            "finalized": inspection.status == CanonicalityInspectionStatus::Finalized,
            "missing": inspection.status == CanonicalityInspectionStatus::Missing,
            "orphaned": inspection.status == CanonicalityInspectionStatus::Orphaned,
        }
    })
}

fn render_manifest_drift_inspection(inspection: &ManifestDriftAlertInspection) -> Value {
    json!({
        "command": "inspect manifest-drift",
        "read_only": true,
        "counts": {
            "manifest_code_hash_drift": inspection.code_hash_drift_alerts.len(),
            "manifest_proxy_implementation": inspection.proxy_implementation_alerts.len(),
            "total": inspection.total_alert_count(),
        },
        "manifest_code_hash_drift_alerts": inspection
            .code_hash_drift_alerts
            .iter()
            .map(render_manifest_code_hash_drift_alert)
            .collect::<Vec<_>>(),
        "proxy_implementation_alerts": inspection
            .proxy_implementation_alerts
            .iter()
            .map(render_manifest_proxy_implementation_alert)
            .collect::<Vec<_>>(),
    })
}

fn render_manifest_code_hash_drift_alert(alert: &ManifestDriftAlertObservation) -> Value {
    json!({
        "normalized_event_id": alert.normalized_event_id,
        "event_identity": alert.event_identity.as_str(),
        "event_kind": alert.alert_kind.event_kind(),
        "alert_type": alert.alert_kind.alert_type(),
        "namespace": alert.namespace.as_str(),
        "source_family": alert.source_family.as_str(),
        "manifest_version": alert.manifest_version,
        "source_manifest_id": alert_source_manifest_id(alert),
        "chain": alert_chain(alert),
        "chain_id": alert.chain_id.as_deref(),
        "canonicality_state": canonicality_state_label(alert.canonicality_state),
        "lifecycle": render_manifest_alert_lifecycle(alert),
        "declaration": {
            "kind": alert_state_string(alert, "declaration_kind"),
            "name": alert_state_string(alert, "declaration_name"),
        },
        "contract": {
            "contract_instance_id": alert_state_string(alert, "contract_instance_id"),
            "address": alert_state_string(alert, "address"),
        },
        "code_hash": {
            "expected": alert_state_string(alert, "expected_code_hash"),
            "observed": alert_state_string(alert, "observed_code_hash"),
            "observed_byte_length": alert_state_i64(alert, "observed_code_byte_length"),
        },
        "observed_block": {
            "number": alert.block_number.or_else(|| alert_state_i64(alert, "observed_block_number")),
            "hash": alert.block_hash.as_deref().or_else(|| alert_state_string(alert, "observed_block_hash")),
            "canonicality_state": alert_state_string(alert, "observed_canonicality_state"),
        },
        "watched_target": {
            "source": alert_state_string(alert, "watched_source"),
            "raw_fact_ref": alert.raw_fact_ref.clone(),
        },
        "timestamps": {
            "observed_at": format_timestamp(alert.observed_at),
        },
        "remediation": alert_remediation(alert),
    })
}

fn render_manifest_proxy_implementation_alert(alert: &ManifestDriftAlertObservation) -> Value {
    json!({
        "normalized_event_id": alert.normalized_event_id,
        "event_identity": alert.event_identity.as_str(),
        "event_kind": alert.alert_kind.event_kind(),
        "alert_type": alert.alert_kind.alert_type(),
        "namespace": alert.namespace.as_str(),
        "source_family": alert.source_family.as_str(),
        "manifest_version": alert.manifest_version,
        "source_manifest_id": alert_source_manifest_id(alert),
        "chain": alert_chain(alert),
        "chain_id": alert.chain_id.as_deref(),
        "canonicality_state": canonicality_state_label(alert.canonicality_state),
        "lifecycle": render_manifest_alert_lifecycle(alert),
        "declaration": {
            "name": alert_state_string(alert, "declaration_name"),
            "role": alert_state_string(alert, "role"),
            "proxy_kind": alert_state_string(alert, "proxy_kind"),
        },
        "proxy": {
            "contract_instance_id": alert_state_string(alert, "proxy_contract_instance_id"),
            "address": alert_state_string(alert, "proxy_address"),
        },
        "implementation": {
            "contract_instance_id": alert_state_string(alert, "implementation_contract_instance_id"),
            "address": alert_state_string(alert, "implementation_address"),
        },
        "implementation_edge": {
            "admission": alert_state_string(alert, "admission"),
            "active_from_block_number": alert_state_i64(alert, "active_from_block_number"),
            "active_to_block_number": alert_state_i64(alert, "active_to_block_number"),
            "provenance": alert.alert_state.get("provenance").cloned().unwrap_or(Value::Null),
        },
        "timestamps": {
            "observed_at": format_timestamp(alert.observed_at),
        },
        "remediation": alert_remediation(alert),
    })
}

fn render_manifest_alert_lifecycle(alert: &ManifestDriftAlertObservation) -> Value {
    let status = alert_state_string(alert, "alert_status").unwrap_or("unknown");
    json!({
        "status": status,
        "active": status == "active",
        "remediated": status == "remediated",
    })
}

fn alert_source_manifest_id(alert: &ManifestDriftAlertObservation) -> Option<i64> {
    alert
        .source_manifest_id
        .or_else(|| alert_state_i64(alert, "source_manifest_id"))
        .or_else(|| {
            alert
                .raw_fact_ref
                .get("manifest_id")
                .and_then(Value::as_i64)
        })
}

fn alert_chain(alert: &ManifestDriftAlertObservation) -> Option<&str> {
    alert_state_string(alert, "chain").or(alert.chain_id.as_deref())
}

fn alert_state_string<'a>(
    alert: &'a ManifestDriftAlertObservation,
    field: &str,
) -> Option<&'a str> {
    alert.alert_state.get(field).and_then(Value::as_str)
}

fn alert_state_i64(alert: &ManifestDriftAlertObservation, field: &str) -> Option<i64> {
    alert.alert_state.get(field).and_then(Value::as_i64)
}

fn alert_remediation(alert: &ManifestDriftAlertObservation) -> Value {
    ["remediation", "remediation_metadata"]
        .iter()
        .find_map(|field| alert.alert_state.get(*field).cloned())
        .unwrap_or(Value::Null)
}

fn render_watch_plan_inspection(
    watched_contracts: &[WatchedContract],
    summary: &WatchedContractSummary,
    watch_plan: &[WatchedChainPlan],
) -> Value {
    json!({
        "command": "inspect watch-plan",
        "read_only": true,
        "counts": {
            "unique_contracts": summary.unique_contract_count,
            "source_entries": summary.source_entry_count,
            "manifest_roots": summary.manifest_root_count,
            "manifest_contracts": summary.manifest_contract_count,
            "discovery_edges": summary.discovery_edge_count,
            "chains": summary.chains.len(),
        },
        "summary": {
            "unique_contract_count": summary.unique_contract_count,
            "source_entry_count": summary.source_entry_count,
            "manifest_root_count": summary.manifest_root_count,
            "manifest_contract_count": summary.manifest_contract_count,
            "discovery_edge_count": summary.discovery_edge_count,
            "chains": summary
                .chains
                .iter()
                .map(render_watched_contract_chain_summary)
                .collect::<Vec<_>>(),
        },
        "watch_plan": watch_plan
            .iter()
            .map(render_watched_chain_plan)
            .collect::<Vec<_>>(),
        "watched_contracts": watched_contracts
            .iter()
            .map(render_watched_contract)
            .collect::<Vec<_>>(),
    })
}

fn render_watched_contract_chain_summary(summary: &WatchedContractChainSummary) -> Value {
    json!({
        "chain": summary.chain.as_str(),
        "unique_contract_count": summary.unique_contract_count,
        "manifest_root_count": summary.manifest_root_count,
        "manifest_contract_count": summary.manifest_contract_count,
        "discovery_edge_count": summary.discovery_edge_count,
    })
}

fn render_watched_chain_plan(plan: &WatchedChainPlan) -> Value {
    json!({
        "chain": plan.chain.as_str(),
        "addresses": plan.addresses.clone(),
        "counts": {
            "unique_contracts": plan.addresses.len(),
            "source_entries": plan.manifest_root_entry_count
                + plan.manifest_contract_entry_count
                + plan.discovery_edge_entry_count,
            "manifest_roots": plan.manifest_root_entry_count,
            "manifest_contracts": plan.manifest_contract_entry_count,
            "discovery_edges": plan.discovery_edge_entry_count,
        },
    })
}

fn render_watched_contract(contract: &WatchedContract) -> Value {
    json!({
        "chain": contract.chain.as_str(),
        "source_family": contract.source_family.as_str(),
        "contract_instance_id": contract.contract_instance_id.to_string(),
        "address": contract.address.as_str(),
        "source": watched_contract_source_label(contract.source),
        "source_manifest_id": contract.source_manifest_id,
        "active_block_range": {
            "from_block_number": contract.active_from_block_number,
            "to_block_number": contract.active_to_block_number,
        },
    })
}

const fn watched_contract_source_label(source: WatchedContractSource) -> &'static str {
    match source {
        WatchedContractSource::ManifestRoot => "manifest_root",
        WatchedContractSource::ManifestContract => "manifest_contract",
        WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}

fn render_stored_lineage_range_inspection(blocks: &[StoredLineageRangeBlock]) -> Value {
    json!({
        "blocks": blocks
            .iter()
            .map(render_stored_lineage_block)
            .collect::<Vec<_>>(),
    })
}

fn render_stored_lineage_block(block: &StoredLineageRangeBlock) -> Value {
    json!({
        "chain_id": block.chain_id.as_str(),
        "block_number": block.block_number,
        "block_hash": block.block_hash.as_str(),
        "parent_hash": block.parent_hash.as_deref(),
        "canonicality_state": canonicality_state_label(block.canonicality_state),
        "timestamp": format_timestamp(block.block_timestamp),
        "logs_bloom": block.logs_bloom.as_ref().map(|bytes| format_bytes_hex(bytes)),
        "transactions_root": block.transactions_root.as_deref(),
        "receipts_root": block.receipts_root.as_deref(),
        "state_root": block.state_root.as_deref(),
    })
}

fn persisted_context_array(context: &Value, keys: &[&str]) -> Value {
    keys.iter()
        .find_map(|key| context.get(*key).filter(|value| value.is_array()))
        .cloned()
        .unwrap_or(Value::Null)
}

fn persisted_digest_metadata(payload: Option<&Value>, keys: &[&str]) -> Option<Value> {
    let payload = payload?;
    keys.iter().find_map(|key| payload.get(*key).cloned())
}

fn persisted_failure_reason(payload: Option<&Value>) -> Option<String> {
    let payload = payload?;
    ["failure_reason", "reason", "message"]
        .iter()
        .find_map(|key| payload.get(*key)?.as_str().map(str::to_owned))
}

fn execution_trace_status(inspection: &ExecutionTraceInspection) -> &'static str {
    let trace = &inspection.trace;
    if trace.failure_payload.is_some() {
        "failed"
    } else if trace.final_payload.is_some() {
        "succeeded"
    } else {
        "unknown"
    }
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn format_bytes_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("0x");
    for byte in bytes {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }
    encoded
}

const fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '?',
    }
}

fn render_raw_fact_counts(counts: &RawFactAuditCounts) -> Value {
    json!({
        "raw_blocks": counts.raw_block_count,
        "raw_code_hashes": counts.raw_code_hash_count,
        "raw_transactions": counts.raw_transaction_count,
        "raw_receipts": counts.raw_receipt_count,
        "raw_logs": counts.raw_log_count,
        "raw_call_snapshots": counts.raw_call_snapshot_count,
        "total": counts.total(),
    })
}

const fn canonicality_inspection_status_label(
    status: CanonicalityInspectionStatus,
) -> &'static str {
    match status {
        CanonicalityInspectionStatus::Missing => "missing",
        CanonicalityInspectionStatus::Observed => "observed",
        CanonicalityInspectionStatus::Canonical => "canonical",
        CanonicalityInspectionStatus::Safe => "safe",
        CanonicalityInspectionStatus::Finalized => "finalized",
        CanonicalityInspectionStatus::Orphaned => "orphaned",
    }
}

const fn canonicality_state_label(state: CanonicalityState) -> &'static str {
    match state {
        CanonicalityState::Observed => "observed",
        CanonicalityState::Canonical => "canonical",
        CanonicalityState::Safe => "safe",
        CanonicalityState::Finalized => "finalized",
        CanonicalityState::Orphaned => "orphaned",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_manifests::{load_repository, sync_repository};
    use bigname_storage::{
        BackfillJobCreate, BackfillRangeSpec, ChainLineageBlock, ExecutionCacheKey,
        ExecutionOutcome, ExecutionTrace, ExecutionTraceInspection, ExecutionTraceStep,
        ManifestDriftAlertKind, NormalizedEvent, upsert_chain_lineage_blocks,
    };
    use serde_json::json;
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use std::{
        path::PathBuf,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: sqlx::PgPool,
        pool: sqlx::PgPool,
        database_name: String,
        database_url: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| bigname_storage::default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for worker inspect tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_worker_inspect_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker inspect tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool_options = base_options.database(&database_name);
            let database_url = pool_options.to_url_lossy().to_string();
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(pool_options)
                .await
                .context("failed to connect worker inspect test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker inspect tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
                database_url,
            })
        }

        fn pool(&self) -> &sqlx::PgPool {
            &self.pool
        }

        fn database_config(&self) -> DatabaseConfig {
            DatabaseConfig {
                database_url: Some(self.database_url.clone()),
                max_connections: 2,
            }
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

    fn backfill_job_create(idempotency_key: &str) -> BackfillJobCreate {
        BackfillJobCreate {
            deployment_profile: "mainnet".to_owned(),
            chain_id: "eth-mainnet".to_owned(),
            source_identity: json!({
                "source_family": "ens_v1_registry_l1",
                "watch_targets": ["0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e"]
            }),
            scan_mode: "logs".to_owned(),
            range_start_block_number: 100,
            range_end_block_number: 120,
            idempotency_key: idempotency_key.to_owned(),
            ranges: vec![
                BackfillRangeSpec {
                    range_start_block_number: 100,
                    range_end_block_number: 109,
                },
                BackfillRangeSpec {
                    range_start_block_number: 110,
                    range_end_block_number: 120,
                },
            ],
        }
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
    }

    fn lease_deadline() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
            .expect("lease deadline must be valid")
    }

    fn lineage_block(
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: parent_hash.map(str::to_owned),
            block_number,
            block_timestamp: timestamp(1_700_000_000 + block_number),
            logs_bloom: Some(vec![block_number as u8]),
            transactions_root: Some(format!("0xtxroot{block_number:02x}")),
            receipts_root: Some(format!("0xrcroot{block_number:02x}")),
            state_root: Some(format!("0xstroot{block_number:02x}")),
            canonicality_state,
        }
    }

    fn lineage_block_with_nullable_fields(
        block_hash: &str,
        block_number: i64,
        canonicality_state: CanonicalityState,
    ) -> ChainLineageBlock {
        ChainLineageBlock {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: timestamp(1_700_000_000 + block_number),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state,
        }
    }

    fn checked_in_manifest_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("manifests")
    }

    fn render_current_watch_plan(watched_contracts: &[WatchedContract]) -> Value {
        let summary = summarize_watched_contracts(watched_contracts);
        let watch_plan = plan_watched_contracts(watched_contracts);
        render_watch_plan_inspection(watched_contracts, &summary, &watch_plan)
    }

    fn execution_trace() -> ExecutionTrace {
        ExecutionTrace {
            execution_trace_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000abc),
            request_type: "verified_resolution".to_owned(),
            request_key: "ens:alice.eth:addr:60".to_owned(),
            namespace: "ens".to_owned(),
            chain_context: json!({
                "requested_positions": [
                    {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_000,
                        "block_hash": "0xabc123"
                    }
                ],
                "topology_version_boundary": {
                    "ethereum-mainnet": 21_000_000
                }
            }),
            manifest_context: json!({
                "manifest_versions": [
                    {
                        "source_family": "ens_execution",
                        "manifest_version": 5
                    }
                ],
                "rollout_boundary": 5
            }),
            contracts_called: json!([
                {
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "selector": "0x9061b923"
                }
            ]),
            gateway_digests: json!([
                {
                    "digest": "sha256:gateway",
                    "content_type": "application/json",
                    "size": 512
                }
            ]),
            final_payload: Some(json!({
                "final_value_digest": {
                    "digest": "sha256:final",
                    "content_type": "application/json",
                    "size": 96
                }
            })),
            failure_payload: None,
            request_metadata: json!({
                "surface": "alice.eth",
                "records": ["addr:60"]
            }),
            finished_at: Some(timestamp(1_700_000_100)),
            steps: vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_declared_topology".to_owned(),
                    input_digest: Some("sha256:topology-in".to_owned()),
                    output_digest: Some("sha256:topology-out".to_owned()),
                    latency_ms: Some(3),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000
                        }
                    }),
                    step_payload: json!({}),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "call_universal_resolver".to_owned(),
                    input_digest: Some("sha256:call-in".to_owned()),
                    output_digest: Some("sha256:call-out".to_owned()),
                    latency_ms: Some(21),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000
                        }
                    }),
                    step_payload: json!({
                        "attachment_digest_metadata": [
                            {
                                "digest": "sha256:ccip-body",
                                "content_type": "application/octet-stream",
                                "size": 1024
                            }
                        ]
                    }),
                },
            ],
        }
    }

    fn execution_outcome(trace: &ExecutionTrace) -> ExecutionOutcome {
        ExecutionOutcome {
            cache_key: ExecutionCacheKey {
                request_key: trace.request_key.clone(),
                requested_chain_positions: json!([{
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_000,
                    "block_hash": "0xabc123"
                }]),
                manifest_versions: json!([{
                    "source_family": "ens_execution",
                    "manifest_version": 5
                }]),
                topology_version_boundary: json!({
                    "logical_name_id": "ens:alice.eth",
                    "resource_id": "0e7ec7ac-e000-0000-0000-00000000aaa1",
                    "normalized_event_id": null,
                    "event_kind": null,
                    "chain_position": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_000,
                        "block_hash": "0xabc123",
                        "timestamp": "2023-11-14T22:15:00Z"
                    }
                }),
                record_version_boundary: json!({
                    "logical_name_id": "ens:alice.eth",
                    "resource_id": "0e7ec7ac-e000-0000-0000-00000000aaa2",
                    "normalized_event_id": null,
                    "event_kind": null,
                    "chain_position": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_000,
                        "block_hash": "0xabc123",
                        "timestamp": "2023-11-14T22:15:00Z"
                    }
                }),
            },
            execution_trace_id: trace.execution_trace_id,
            request_type: trace.request_type.clone(),
            namespace: trace.namespace.clone(),
            outcome_payload: Some(json!({
                "status": "success"
            })),
            failure_payload: None,
            finished_at: trace
                .finished_at
                .expect("execution trace fixture must finish"),
        }
    }

    fn manifest_code_hash_alert_observation() -> ManifestDriftAlertObservation {
        ManifestDriftAlertObservation {
            normalized_event_id: 101,
            event_identity: "manifest_alert:code_hash".to_owned(),
            alert_kind: ManifestDriftAlertKind::CodeHashDrift,
            namespace: "ens".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 7,
            source_manifest_id: Some(42),
            chain_id: Some("eth-mainnet".to_owned()),
            block_number: Some(123),
            block_hash: Some("0xalertblock".to_owned()),
            raw_fact_ref: json!({
                "manifest_id": 42,
                "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
                "address": "0xregistry",
                "observed_block_number": 123,
                "observed_block_hash": "0xalertblock"
            }),
            canonicality_state: CanonicalityState::Canonical,
            alert_state: json!({
                "alert_type": "manifest_code_hash_drift",
                "alert_status": "active",
                "chain": "eth-mainnet",
                "source_family": "ens_v1_registry_l1",
                "declaration_kind": "contract",
                "declaration_name": "registry",
                "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
                "address": "0xregistry",
                "expected_code_hash": "0xexpected",
                "observed_code_hash": "0xobserved",
                "observed_code_byte_length": 512,
                "observed_block_number": 123,
                "observed_block_hash": "0xalertblock",
                "observed_canonicality_state": "canonical",
                "watched_source": "manifest_contract",
                "source_manifest_id": 42
            }),
            observed_at: timestamp(1_700_000_200),
        }
    }

    fn manifest_proxy_alert_observation() -> ManifestDriftAlertObservation {
        ManifestDriftAlertObservation {
            normalized_event_id: 102,
            event_identity: "manifest_alert:proxy".to_owned(),
            alert_kind: ManifestDriftAlertKind::ProxyImplementation,
            namespace: "ens".to_owned(),
            source_family: "ens_v1_wrapper_l1".to_owned(),
            manifest_version: 9,
            source_manifest_id: None,
            chain_id: Some("eth-mainnet".to_owned()),
            block_number: None,
            block_hash: None,
            raw_fact_ref: json!({
                "manifest_id": 43,
                "discovery_edge_id": 99,
                "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
                "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333"
            }),
            canonicality_state: CanonicalityState::Finalized,
            alert_state: json!({
                "alert_type": "manifest_proxy_implementation_edge",
                "alert_status": "active",
                "chain": "eth-mainnet",
                "source_family": "ens_v1_wrapper_l1",
                "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
                "proxy_address": "0xproxy",
                "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333",
                "implementation_address": "0ximpl",
                "declaration_name": "name_wrapper",
                "role": "name_wrapper",
                "proxy_kind": "eip1967",
                "admission": "observed",
                "active_from_block_number": 120,
                "active_to_block_number": null,
                "provenance": {
                    "slot": "eip1967.proxy.implementation"
                }
            }),
            observed_at: timestamp(1_700_000_240),
        }
    }

    fn manifest_code_hash_alert_event(event_identity: &str) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "ManifestCodeHashDriftAlert".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 7,
            source_manifest_id: None,
            chain_id: Some("eth-mainnet".to_owned()),
            block_number: Some(123),
            block_hash: Some("0xalertblock".to_owned()),
            transaction_hash: None,
            log_index: None,
            raw_fact_ref: json!({
                "manifest_id": 42,
                "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
                "address": "0xregistry",
                "observed_block_number": 123,
                "observed_block_hash": "0xalertblock"
            }),
            derivation_kind: "manifest_alert".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({
                "alert_type": "manifest_code_hash_drift",
                "alert_status": "active",
                "chain": "eth-mainnet",
                "source_family": "ens_v1_registry_l1",
                "declaration_kind": "contract",
                "declaration_name": "registry",
                "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
                "address": "0xregistry",
                "expected_code_hash": "0xexpected",
                "observed_code_hash": "0xobserved",
                "observed_code_byte_length": 512,
                "observed_block_number": 123,
                "observed_block_hash": "0xalertblock",
                "observed_canonicality_state": "canonical",
                "watched_source": "manifest_contract",
                "source_manifest_id": 42
            }),
        }
    }

    fn manifest_proxy_alert_event(event_identity: &str) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "ManifestProxyImplementationAlert".to_owned(),
            source_family: "ens_v1_wrapper_l1".to_owned(),
            manifest_version: 9,
            source_manifest_id: None,
            chain_id: Some("eth-mainnet".to_owned()),
            block_number: None,
            block_hash: None,
            transaction_hash: None,
            log_index: None,
            raw_fact_ref: json!({
                "manifest_id": 43,
                "discovery_edge_id": 99,
                "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
                "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333"
            }),
            derivation_kind: "manifest_alert".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "alert_type": "manifest_proxy_implementation_edge",
                "alert_status": "active",
                "chain": "eth-mainnet",
                "source_family": "ens_v1_wrapper_l1",
                "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
                "proxy_address": "0xproxy",
                "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333",
                "implementation_address": "0ximpl",
                "declaration_name": "name_wrapper",
                "role": "name_wrapper",
                "proxy_kind": "eip1967",
                "admission": "observed",
                "active_from_block_number": 120,
                "active_to_block_number": null,
                "provenance": {
                    "slot": "eip1967.proxy.implementation"
                }
            }),
        }
    }

    #[test]
    fn renders_backfill_job_inspection_json() {
        let inspection = BackfillJobRecord {
            job: BackfillJob {
                backfill_job_id: 42,
                deployment_profile: "mainnet".to_owned(),
                chain_id: "eth-mainnet".to_owned(),
                source_identity: json!({
                    "source_family": "ens_v1_registry_l1",
                    "watch_targets": ["0xregistry"]
                }),
                scan_mode: "logs".to_owned(),
                range_start_block_number: 100,
                range_end_block_number: 120,
                idempotency_key: "job-json-shape".to_owned(),
                status: BackfillLifecycleStatus::Running,
                failure_reason: None,
                failure_metadata: json!({}),
                created_at: timestamp(1_700_000_000),
                updated_at: timestamp(1_700_000_030),
                completed_at: None,
            },
            ranges: vec![
                BackfillRange {
                    backfill_range_id: 7,
                    backfill_job_id: 42,
                    range_start_block_number: 100,
                    range_end_block_number: 109,
                    checkpoint_block_number: 105,
                    status: BackfillLifecycleStatus::Running,
                    lease_token: Some("lease-a".to_owned()),
                    lease_owner: Some("worker-a".to_owned()),
                    lease_expires_at: Some(timestamp(1_700_000_300)),
                    attempt_count: 2,
                    failure_reason: None,
                    failure_metadata: json!({}),
                    created_at: timestamp(1_700_000_000),
                    updated_at: timestamp(1_700_000_040),
                    completed_at: None,
                },
                BackfillRange {
                    backfill_range_id: 8,
                    backfill_job_id: 42,
                    range_start_block_number: 110,
                    range_end_block_number: 120,
                    checkpoint_block_number: 110,
                    status: BackfillLifecycleStatus::Failed,
                    lease_token: None,
                    lease_owner: None,
                    lease_expires_at: None,
                    attempt_count: 1,
                    failure_reason: Some("rpc timeout".to_owned()),
                    failure_metadata: json!({ "block": 111 }),
                    created_at: timestamp(1_700_000_000),
                    updated_at: timestamp(1_700_000_050),
                    completed_at: None,
                },
            ],
        };

        let rendered = render_backfill_job_inspection(&inspection);

        assert_eq!(rendered["job"]["backfill_job_id"], 42);
        assert_eq!(rendered["job"]["deployment_profile"], "mainnet");
        assert_eq!(rendered["job"]["chain_id"], "eth-mainnet");
        assert_eq!(
            rendered["job"]["source_identity"]["source_family"],
            "ens_v1_registry_l1"
        );
        assert_eq!(rendered["job"]["scan_mode"], "logs");
        assert_eq!(rendered["job"]["status"], "running");
        assert_eq!(rendered["job"]["lifecycle"]["running"], true);
        assert_eq!(rendered["job"]["lifecycle"]["completed"], false);
        assert_eq!(rendered["job"]["declared_range"]["start_block_number"], 100);
        assert_eq!(rendered["job"]["declared_range"]["end_block_number"], 120);
        assert_eq!(rendered["job"]["idempotency_key"], "job-json-shape");
        assert_eq!(
            rendered["job"]["timestamps"]["created_at"],
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            rendered["job"]["timestamps"]["updated_at"],
            "2023-11-14T22:13:50Z"
        );
        assert!(rendered["job"]["timestamps"]["completed_at"].is_null());
        assert!(rendered["job"]["failure"]["reason"].is_null());
        assert_eq!(rendered["job"]["failure"]["metadata"], json!({}));

        assert_eq!(
            rendered["ranges"]
                .as_array()
                .expect("ranges must be an array")
                .len(),
            2
        );
        assert_eq!(rendered["ranges"][0]["backfill_range_id"], 7);
        assert_eq!(rendered["ranges"][0]["backfill_job_id"], 42);
        assert_eq!(rendered["ranges"][0]["status"], "running");
        assert_eq!(
            rendered["ranges"][0]["declared_range"]["start_block_number"],
            100
        );
        assert_eq!(
            rendered["ranges"][0]["declared_range"]["end_block_number"],
            109
        );
        assert_eq!(rendered["ranges"][0]["checkpoint"]["block_number"], 105);
        assert_eq!(rendered["ranges"][0]["lease"]["owner"], "worker-a");
        assert_eq!(rendered["ranges"][0]["lease"]["token"], "lease-a");
        assert_eq!(
            rendered["ranges"][0]["lease"]["expires_at"],
            "2023-11-14T22:18:20Z"
        );
        assert_eq!(rendered["ranges"][0]["attempt_count"], 2);
        assert_eq!(rendered["ranges"][1]["status"], "failed");
        assert_eq!(rendered["ranges"][1]["lifecycle"]["failed"], true);
        assert!(rendered["ranges"][1]["lease"]["owner"].is_null());
        assert!(rendered["ranges"][1]["lease"]["token"].is_null());
        assert!(rendered["ranges"][1]["lease"]["expires_at"].is_null());
        assert_eq!(rendered["ranges"][1]["failure"]["reason"], "rpc timeout");
        assert_eq!(
            rendered["ranges"][1]["failure"]["metadata"],
            json!({ "block": 111 })
        );
    }

    #[test]
    fn renders_canonicality_inspection_json() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xabc".to_owned(),
            status: CanonicalityInspectionStatus::Safe,
            lineage_state: Some(CanonicalityState::Safe),
            parent_hash: Some("0xparent".to_owned()),
            block_number: Some(123),
            raw_fact_counts: RawFactAuditCounts {
                raw_block_count: 1,
                raw_code_hash_count: 2,
                raw_transaction_count: 3,
                raw_receipt_count: 4,
                raw_log_count: 5,
                raw_call_snapshot_count: 6,
            },
            normalized_event_count: 7,
        });

        assert_eq!(rendered["chain_id"], "eth-mainnet");
        assert_eq!(rendered["block_hash"], "0xabc");
        assert_eq!(rendered["status"], "safe");
        assert_eq!(rendered["lineage_canonicality"], "safe");
        assert_eq!(rendered["parent_hash"], "0xparent");
        assert_eq!(rendered["block_number"], 123);
        assert_eq!(rendered["raw_fact_counts"]["raw_blocks"], 1);
        assert_eq!(rendered["raw_fact_counts"]["raw_code_hashes"], 2);
        assert_eq!(rendered["raw_fact_counts"]["raw_transactions"], 3);
        assert_eq!(rendered["raw_fact_counts"]["raw_receipts"], 4);
        assert_eq!(rendered["raw_fact_counts"]["raw_logs"], 5);
        assert_eq!(rendered["raw_fact_counts"]["raw_call_snapshots"], 6);
        assert_eq!(rendered["raw_fact_counts"]["total"], 21);
        assert_eq!(rendered["normalized_event_count"], 7);
        assert_eq!(rendered["states"]["safe"], true);
        assert_eq!(rendered["states"]["canonical"], false);
    }

    #[test]
    fn renders_missing_lineage_as_nulls() {
        let rendered = render_canonicality_inspection(&CanonicalityInspection {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xmissing".to_owned(),
            status: CanonicalityInspectionStatus::Missing,
            lineage_state: None,
            parent_hash: None,
            block_number: None,
            raw_fact_counts: RawFactAuditCounts::default(),
            normalized_event_count: 0,
        });

        assert_eq!(rendered["status"], "missing");
        assert!(rendered["lineage_canonicality"].is_null());
        assert!(rendered["parent_hash"].is_null());
        assert!(rendered["block_number"].is_null());
        assert_eq!(rendered["raw_fact_counts"]["total"], 0);
        assert_eq!(rendered["states"]["missing"], true);
        assert_eq!(rendered["states"]["orphaned"], false);
    }

    #[test]
    fn renders_stored_lineage_range_json() {
        let blocks = vec![
            lineage_block("0x010", None, 10, CanonicalityState::Canonical),
            lineage_block_with_nullable_fields("0x012", 12, CanonicalityState::Observed),
        ];

        let rendered = render_stored_lineage_range_inspection(&blocks);

        assert_eq!(
            rendered["blocks"]
                .as_array()
                .expect("blocks must be an array")
                .len(),
            2
        );
        assert_eq!(rendered["blocks"][0]["chain_id"], "eth-mainnet");
        assert_eq!(rendered["blocks"][0]["block_number"], 10);
        assert_eq!(rendered["blocks"][0]["block_hash"], "0x010");
        assert!(rendered["blocks"][0]["parent_hash"].is_null());
        assert_eq!(rendered["blocks"][0]["canonicality_state"], "canonical");
        assert_eq!(rendered["blocks"][0]["timestamp"], "2023-11-14T22:13:30Z");
        assert_eq!(rendered["blocks"][0]["logs_bloom"], "0x0a");
        assert_eq!(rendered["blocks"][0]["transactions_root"], "0xtxroot0a");
        assert_eq!(rendered["blocks"][0]["receipts_root"], "0xrcroot0a");
        assert_eq!(rendered["blocks"][0]["state_root"], "0xstroot0a");

        assert_eq!(rendered["blocks"][1]["canonicality_state"], "observed");
        assert!(rendered["blocks"][1]["parent_hash"].is_null());
        assert!(rendered["blocks"][1]["logs_bloom"].is_null());
        assert!(rendered["blocks"][1]["transactions_root"].is_null());
        assert!(rendered["blocks"][1]["receipts_root"].is_null());
        assert!(rendered["blocks"][1]["state_root"].is_null());
    }

    #[test]
    fn renders_execution_trace_inspection_json() {
        let trace = execution_trace();
        let rendered = render_execution_trace_inspection(&ExecutionTraceInspection {
            trace: trace.clone(),
        });

        assert_eq!(rendered["command"], "inspect execution-trace");
        assert_eq!(
            rendered["execution_trace_id"],
            trace.execution_trace_id.to_string()
        );
        assert_eq!(rendered["request_type"], "verified_resolution");
        assert_eq!(rendered["request_key"], "ens:alice.eth:addr:60");
        assert_eq!(rendered["namespace"], "ens");
        assert_eq!(rendered["request"]["type"], "verified_resolution");
        assert_eq!(rendered["request"]["key"], "ens:alice.eth:addr:60");
        assert_eq!(rendered["request_metadata"]["surface"], "alice.eth");
        assert_eq!(
            rendered["chain_positions"][0]["chain_id"],
            "ethereum-mainnet"
        );
        assert_eq!(
            rendered["manifest_versions"][0]["source_family"],
            "ens_execution"
        );
        assert_eq!(
            rendered["contracts_called"][0]["contract_address"],
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );
        assert_eq!(rendered["gateway_digests"][0]["digest"], "sha256:gateway");
        assert_eq!(rendered["status"], "succeeded");
        assert_eq!(rendered["final_value_digest"]["digest"], "sha256:final");
        assert!(rendered["failure_reason"].is_null());
        assert_eq!(rendered["finished_at"], "2023-11-14T22:15:00Z");

        assert_eq!(
            rendered["steps"]
                .as_array()
                .expect("steps must be an array")
                .len(),
            2
        );
        assert_eq!(rendered["steps"][0]["step_index"], 0);
        assert_eq!(rendered["steps"][0]["step_kind"], "load_declared_topology");
        assert_eq!(rendered["steps"][0]["input_digest"], "sha256:topology-in");
        assert_eq!(rendered["steps"][0]["output_digest"], "sha256:topology-out");
        assert_eq!(rendered["steps"][0]["latency_ms"], 3);
        assert_eq!(
            rendered["steps"][0]["canonicality_dependency"]["ethereum-mainnet"]["block_hash"],
            "0xabc123"
        );
        assert!(rendered["steps"][0]["attachment_digest_metadata"].is_null());

        assert_eq!(rendered["steps"][1]["step_index"], 1);
        assert_eq!(rendered["steps"][1]["step_kind"], "call_universal_resolver");
        assert_eq!(
            rendered["steps"][1]["attachment_digest_metadata"][0]["digest"],
            "sha256:ccip-body"
        );
    }

    #[test]
    fn renders_manifest_drift_inspection_json() {
        let rendered = render_manifest_drift_inspection(&ManifestDriftAlertInspection {
            code_hash_drift_alerts: vec![manifest_code_hash_alert_observation()],
            proxy_implementation_alerts: vec![manifest_proxy_alert_observation()],
        });

        assert_eq!(rendered["command"], "inspect manifest-drift");
        assert_eq!(rendered["read_only"], true);
        assert_eq!(rendered["counts"]["manifest_code_hash_drift"], 1);
        assert_eq!(rendered["counts"]["manifest_proxy_implementation"], 1);
        assert_eq!(rendered["counts"]["total"], 2);

        let code_alert = &rendered["manifest_code_hash_drift_alerts"][0];
        assert_eq!(code_alert["normalized_event_id"], 101);
        assert_eq!(code_alert["event_identity"], "manifest_alert:code_hash");
        assert_eq!(code_alert["event_kind"], "ManifestCodeHashDriftAlert");
        assert_eq!(code_alert["alert_type"], "manifest_code_hash_drift");
        assert_eq!(code_alert["namespace"], "ens");
        assert_eq!(code_alert["source_family"], "ens_v1_registry_l1");
        assert_eq!(code_alert["manifest_version"], 7);
        assert_eq!(code_alert["source_manifest_id"], 42);
        assert_eq!(code_alert["chain"], "eth-mainnet");
        assert_eq!(code_alert["chain_id"], "eth-mainnet");
        assert_eq!(code_alert["canonicality_state"], "canonical");
        assert_eq!(code_alert["lifecycle"]["status"], "active");
        assert_eq!(code_alert["lifecycle"]["active"], true);
        assert_eq!(code_alert["declaration"]["kind"], "contract");
        assert_eq!(code_alert["declaration"]["name"], "registry");
        assert_eq!(
            code_alert["contract"]["contract_instance_id"],
            "0e7ec7ac-e000-0000-0000-000000000111"
        );
        assert_eq!(code_alert["contract"]["address"], "0xregistry");
        assert_eq!(code_alert["code_hash"]["expected"], "0xexpected");
        assert_eq!(code_alert["code_hash"]["observed"], "0xobserved");
        assert_eq!(code_alert["code_hash"]["observed_byte_length"], 512);
        assert_eq!(code_alert["observed_block"]["number"], 123);
        assert_eq!(code_alert["observed_block"]["hash"], "0xalertblock");
        assert_eq!(
            code_alert["observed_block"]["canonicality_state"],
            "canonical"
        );
        assert_eq!(code_alert["watched_target"]["source"], "manifest_contract");
        assert_eq!(
            code_alert["watched_target"]["raw_fact_ref"]["manifest_id"],
            42
        );
        assert_eq!(
            code_alert["timestamps"]["observed_at"],
            "2023-11-14T22:16:40Z"
        );
        assert!(code_alert["remediation"].is_null());

        let proxy_alert = &rendered["proxy_implementation_alerts"][0];
        assert_eq!(proxy_alert["normalized_event_id"], 102);
        assert_eq!(proxy_alert["event_identity"], "manifest_alert:proxy");
        assert_eq!(
            proxy_alert["event_kind"],
            "ManifestProxyImplementationAlert"
        );
        assert_eq!(
            proxy_alert["alert_type"],
            "manifest_proxy_implementation_edge"
        );
        assert_eq!(proxy_alert["namespace"], "ens");
        assert_eq!(proxy_alert["source_family"], "ens_v1_wrapper_l1");
        assert_eq!(proxy_alert["manifest_version"], 9);
        assert_eq!(proxy_alert["source_manifest_id"], 43);
        assert_eq!(proxy_alert["chain"], "eth-mainnet");
        assert_eq!(proxy_alert["canonicality_state"], "finalized");
        assert_eq!(proxy_alert["declaration"]["name"], "name_wrapper");
        assert_eq!(proxy_alert["declaration"]["role"], "name_wrapper");
        assert_eq!(proxy_alert["declaration"]["proxy_kind"], "eip1967");
        assert_eq!(
            proxy_alert["proxy"]["contract_instance_id"],
            "0e7ec7ac-e000-0000-0000-000000000222"
        );
        assert_eq!(proxy_alert["proxy"]["address"], "0xproxy");
        assert_eq!(
            proxy_alert["implementation"]["contract_instance_id"],
            "0e7ec7ac-e000-0000-0000-000000000333"
        );
        assert_eq!(proxy_alert["implementation"]["address"], "0ximpl");
        assert_eq!(proxy_alert["implementation_edge"]["admission"], "observed");
        assert_eq!(
            proxy_alert["implementation_edge"]["active_from_block_number"],
            120
        );
        assert!(proxy_alert["implementation_edge"]["active_to_block_number"].is_null());
        assert_eq!(
            proxy_alert["implementation_edge"]["provenance"]["slot"],
            "eip1967.proxy.implementation"
        );
        assert_eq!(
            proxy_alert["timestamps"]["observed_at"],
            "2023-11-14T22:17:20Z"
        );
        assert!(proxy_alert["remediation"].is_null());
    }

    #[test]
    fn renders_inspect_watch_plan_json_shape() {
        let watched_contracts = vec![
            WatchedContract {
                chain: "base-mainnet".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                contract_instance_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000101),
                source: WatchedContractSource::ManifestRoot,
                source_manifest_id: Some(11),
                active_from_block_number: Some(100),
                active_to_block_number: None,
            },
            WatchedContract {
                chain: "base-mainnet".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                contract_instance_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000102),
                source: WatchedContractSource::ManifestContract,
                source_manifest_id: Some(11),
                active_from_block_number: Some(100),
                active_to_block_number: Some(200),
            },
            WatchedContract {
                chain: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_resolver_l1".to_owned(),
                address: "0x0000000000000000000000000000000000000002".to_owned(),
                contract_instance_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000103),
                source: WatchedContractSource::DiscoveryEdge,
                source_manifest_id: None,
                active_from_block_number: None,
                active_to_block_number: None,
            },
        ];
        let rendered = render_current_watch_plan(&watched_contracts);

        assert_eq!(rendered["command"], "inspect watch-plan");
        assert_eq!(rendered["read_only"], true);
        assert_eq!(rendered["counts"]["unique_contracts"], 2);
        assert_eq!(rendered["counts"]["source_entries"], 3);
        assert_eq!(rendered["counts"]["manifest_roots"], 1);
        assert_eq!(rendered["counts"]["manifest_contracts"], 1);
        assert_eq!(rendered["counts"]["discovery_edges"], 1);
        assert_eq!(rendered["counts"]["chains"], 2);

        assert_eq!(rendered["summary"]["unique_contract_count"], 2);
        assert_eq!(rendered["summary"]["source_entry_count"], 3);
        assert_eq!(rendered["summary"]["chains"][0]["chain"], "base-mainnet");
        assert_eq!(rendered["summary"]["chains"][0]["unique_contract_count"], 1);
        assert_eq!(rendered["summary"]["chains"][0]["manifest_root_count"], 1);
        assert_eq!(
            rendered["summary"]["chains"][0]["manifest_contract_count"],
            1
        );
        assert_eq!(rendered["summary"]["chains"][0]["discovery_edge_count"], 0);
        assert_eq!(
            rendered["summary"]["chains"][1]["chain"],
            "ethereum-mainnet"
        );
        assert_eq!(rendered["summary"]["chains"][1]["discovery_edge_count"], 1);

        let base_plan = &rendered["watch_plan"][0];
        assert_eq!(base_plan["chain"], "base-mainnet");
        assert_eq!(
            base_plan["addresses"],
            json!(["0x0000000000000000000000000000000000000001"])
        );
        assert_eq!(base_plan["counts"]["unique_contracts"], 1);
        assert_eq!(base_plan["counts"]["source_entries"], 2);
        assert_eq!(base_plan["counts"]["manifest_roots"], 1);
        assert_eq!(base_plan["counts"]["manifest_contracts"], 1);
        assert_eq!(base_plan["counts"]["discovery_edges"], 0);

        let root_contract = &rendered["watched_contracts"][0];
        assert_eq!(root_contract["chain"], "base-mainnet");
        assert_eq!(root_contract["source_family"], "basenames_base_registry");
        assert_eq!(
            root_contract["contract_instance_id"],
            "0e7ec7ac-e000-0000-0000-000000000101"
        );
        assert_eq!(
            root_contract["address"],
            "0x0000000000000000000000000000000000000001"
        );
        assert_eq!(root_contract["source"], "manifest_root");
        assert_eq!(root_contract["source_manifest_id"], 11);
        assert_eq!(
            root_contract["active_block_range"]["from_block_number"],
            100
        );
        assert!(root_contract["active_block_range"]["to_block_number"].is_null());

        let manifest_contract = &rendered["watched_contracts"][1];
        assert_eq!(manifest_contract["source"], "manifest_contract");
        assert_eq!(
            manifest_contract["active_block_range"]["to_block_number"],
            200
        );

        let discovery_contract = &rendered["watched_contracts"][2];
        assert_eq!(discovery_contract["chain"], "ethereum-mainnet");
        assert_eq!(discovery_contract["source"], "discovery_edge");
        assert!(discovery_contract["source_manifest_id"].is_null());
        assert!(discovery_contract["active_block_range"]["from_block_number"].is_null());
        assert!(discovery_contract["active_block_range"]["to_block_number"].is_null());
    }

    #[tokio::test]
    async fn inspect_stored_lineage_range_orders_and_bounds_stored_rows() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block("0x012b", Some("0x010"), 12, CanonicalityState::Safe),
                lineage_block("0x009", None, 9, CanonicalityState::Canonical),
                lineage_block("0x010", None, 10, CanonicalityState::Canonical),
                lineage_block("0x013", Some("0x012b"), 13, CanonicalityState::Finalized),
                lineage_block("0x012a", Some("0x010"), 12, CanonicalityState::Orphaned),
                ChainLineageBlock {
                    chain_id: "base-mainnet".to_owned(),
                    ..lineage_block(
                        "0x011-base",
                        Some("0x010"),
                        11,
                        CanonicalityState::Canonical,
                    )
                },
            ],
        )
        .await?;

        let blocks =
            bigname_storage::list_stored_lineage_range(database.pool(), "eth-mainnet", 10, 12)
                .await?;
        let rendered = render_stored_lineage_range_inspection(&blocks);

        assert_eq!(
            rendered["blocks"]
                .as_array()
                .expect("blocks must be an array")
                .iter()
                .map(|block| {
                    (
                        block["block_number"].as_i64().expect("block number"),
                        block["block_hash"].as_str().expect("block hash").to_owned(),
                        block["canonicality_state"]
                            .as_str()
                            .expect("canonicality state")
                            .to_owned(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (10, "0x010".to_owned(), "canonical".to_owned()),
                (12, "0x012a".to_owned(), "orphaned".to_owned()),
                (12, "0x012b".to_owned(), "safe".to_owned()),
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_watch_plan_does_not_mutate_watched_contract_state() -> Result<()> {
        let database = TestDatabase::new().await?;
        let repository = load_repository(checked_in_manifest_root())?;
        let sync_summary = sync_repository(database.pool(), &repository).await?;
        assert!(sync_summary.active_manifest_count > 0);

        let before_contracts = load_watched_contracts(database.pool()).await?;
        assert!(!before_contracts.is_empty());
        let before = render_current_watch_plan(&before_contracts);

        inspect_watch_plan(InspectWatchPlanArgs {
            database: database.database_config(),
            json: true,
        })
        .await?;

        let after_contracts = load_watched_contracts(database.pool()).await?;
        let after = render_current_watch_plan(&after_contracts);
        assert_eq!(after, before);

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_backfill_job_missing_job_returns_error() -> Result<()> {
        let database = TestDatabase::new().await?;

        let error = inspect_backfill_job(InspectBackfillJobArgs {
            database: database.database_config(),
            backfill_job_id: 9_999_999,
        })
        .await
        .expect_err("missing backfill job inspection must fail");
        assert!(
            error.to_string().contains("missing backfill job 9999999"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_execution_trace_missing_trace_returns_error() -> Result<()> {
        let database = TestDatabase::new().await?;

        let missing_id = Uuid::from_u128(0x0e7ec7ace00000000000000000009999);
        let error = inspect_execution_trace(InspectExecutionTraceArgs {
            database: database.database_config(),
            execution_trace_id: missing_id,
            json: true,
        })
        .await
        .expect_err("missing execution trace inspection must fail");
        assert!(
            error
                .to_string()
                .contains(&format!("missing execution trace {missing_id}")),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_backfill_job_does_not_mutate_backfill_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let created = bigname_storage::create_backfill_job(
            database.pool(),
            &backfill_job_create("worker-inspect-readonly"),
        )
        .await?;
        let reserved = bigname_storage::reserve_backfill_range(
            database.pool(),
            created.job.backfill_job_id,
            "worker-a",
            "lease-a",
            lease_deadline(),
        )
        .await?
        .expect("range must be reservable");
        bigname_storage::advance_backfill_range(
            database.pool(),
            reserved.backfill_range_id,
            "lease-a",
            105,
        )
        .await?;

        let before =
            load_backfill_job_inspection(database.pool(), created.job.backfill_job_id).await?;

        inspect_backfill_job(InspectBackfillJobArgs {
            database: database.database_config(),
            backfill_job_id: created.job.backfill_job_id,
        })
        .await?;

        let after =
            load_backfill_job_inspection(database.pool(), created.job.backfill_job_id).await?;
        assert_eq!(after, before);

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_execution_trace_does_not_mutate_execution_storage() -> Result<()> {
        let database = TestDatabase::new().await?;
        let trace = execution_trace();
        let outcome = execution_outcome(&trace);
        bigname_storage::upsert_execution_trace(database.pool(), &trace).await?;
        bigname_storage::upsert_execution_outcome(database.pool(), &outcome).await?;

        let before_trace = bigname_storage::load_execution_trace_inspection(
            database.pool(),
            trace.execution_trace_id,
        )
        .await?;
        let before_outcome =
            bigname_storage::load_execution_outcome(database.pool(), &outcome.cache_key).await?;

        inspect_execution_trace(InspectExecutionTraceArgs {
            database: database.database_config(),
            execution_trace_id: trace.execution_trace_id,
            json: true,
        })
        .await?;

        let after_trace = bigname_storage::load_execution_trace_inspection(
            database.pool(),
            trace.execution_trace_id,
        )
        .await?;
        let after_outcome =
            bigname_storage::load_execution_outcome(database.pool(), &outcome.cache_key).await?;

        assert_eq!(after_trace, before_trace);
        assert_eq!(after_outcome, before_outcome);

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_manifest_drift_does_not_mutate_alert_observations() -> Result<()> {
        let database = TestDatabase::new().await?;
        bigname_storage::upsert_normalized_events(
            database.pool(),
            &[
                manifest_code_hash_alert_event("manifest_alert:inspect:code"),
                manifest_proxy_alert_event("manifest_alert:inspect:proxy"),
            ],
        )
        .await?;

        let before =
            bigname_storage::list_manifest_drift_alert_observations(database.pool()).await?;

        inspect_manifest_drift(InspectManifestDriftArgs {
            database: database.database_config(),
            json: true,
        })
        .await?;

        let after =
            bigname_storage::list_manifest_drift_alert_observations(database.pool()).await?;
        assert_eq!(after, before);

        database.cleanup().await
    }

    #[tokio::test]
    async fn inspect_stored_lineage_range_does_not_mutate_lineage_or_checkpoints() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block("0x010", None, 10, CanonicalityState::Canonical),
                lineage_block("0x011", Some("0x010"), 11, CanonicalityState::Safe),
            ],
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO chain_checkpoints (
                chain_id,
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind("eth-mainnet")
        .bind("0x010")
        .bind(10_i64)
        .bind("0x011")
        .bind(11_i64)
        .execute(database.pool())
        .await?;

        let before_lineage =
            bigname_storage::list_stored_lineage_range(database.pool(), "eth-mainnet", 10, 11)
                .await?;
        let before_checkpoints =
            load_chain_checkpoint_snapshot(database.pool(), "eth-mainnet").await?;

        inspect_stored_lineage_range(InspectStoredLineageRangeArgs {
            database: database.database_config(),
            chain_id: "eth-mainnet".to_owned(),
            range_start_block_number: 10,
            range_end_block_number: 11,
        })
        .await?;

        let after_lineage =
            bigname_storage::list_stored_lineage_range(database.pool(), "eth-mainnet", 10, 11)
                .await?;
        let after_checkpoints =
            load_chain_checkpoint_snapshot(database.pool(), "eth-mainnet").await?;

        assert_eq!(after_lineage, before_lineage);
        assert_eq!(after_checkpoints, before_checkpoints);

        database.cleanup().await
    }

    async fn load_chain_checkpoint_snapshot(
        pool: &sqlx::PgPool,
        chain_id: &str,
    ) -> Result<Option<(Option<String>, Option<i64>, Option<String>, Option<i64>)>> {
        let snapshot =
            sqlx::query_as::<_, (Option<String>, Option<i64>, Option<String>, Option<i64>)>(
                r#"
            SELECT
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number
            FROM chain_checkpoints
            WHERE chain_id = $1
            "#,
            )
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;

        Ok(snapshot)
    }
}
