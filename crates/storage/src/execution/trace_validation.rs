use anyhow::{Context, Result, bail};
use serde_json::Value;
use uuid::Uuid;

use super::types::{ExecutionTrace, ExecutionTraceStep};

pub(super) fn validate_execution_trace(trace: &ExecutionTrace) -> Result<()> {
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

pub(super) fn ensure_execution_trace_identity_matches(
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
    match step.latency_ms {
        Some(latency_ms) if latency_ms < 0 => {
            bail!(
                "execution trace {} step {} has negative latency_ms {}",
                execution_trace_id,
                step.step_index,
                latency_ms
            );
        }
        Some(_) => {}
        None => {
            bail!(
                "execution trace {} step {} must set latency_ms",
                execution_trace_id,
                step.step_index
            );
        }
    }
    ensure_nonempty_json_object(
        &step.canonicality_dependency,
        "canonicality_dependency",
        execution_trace_id,
    )?;
    ensure_json_object(&step.step_payload, "step_payload", execution_trace_id)?;

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
