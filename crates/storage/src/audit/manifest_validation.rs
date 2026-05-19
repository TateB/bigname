use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{CanonicalityState, evm_primitives::normalize_optional_evm_b256};

use super::{
    manifest_json::ensure_json_object,
    manifest_state::manifest_alert_state_from_create,
    types::{
        ManifestDriftAlertKind, ManifestDriftAlertLifecycleStatus, ManifestDriftAlertObservation,
        ManifestDriftAlertObservationCreate,
    },
};

pub(super) fn validate_manifest_drift_alert_observation_create(
    observation: &ManifestDriftAlertObservationCreate,
) -> Result<()> {
    if observation.observation_identity.trim().is_empty() {
        bail!("manifest drift alert observation_identity must not be empty");
    }
    if observation.namespace.trim().is_empty() {
        bail!("manifest drift alert namespace must not be empty");
    }
    if observation.source_family.trim().is_empty() {
        bail!("manifest drift alert source_family must not be empty");
    }
    if observation.manifest_version <= 0 {
        bail!(
            "manifest drift alert {} has non-positive manifest_version {}",
            observation.observation_identity,
            observation.manifest_version
        );
    }
    if observation.chain_id.trim().is_empty() {
        bail!("manifest drift alert chain_id must not be empty");
    }
    if observation
        .observed_code_byte_length
        .is_some_and(|value| value < 0)
    {
        bail!(
            "manifest drift alert {} has negative observed_code_byte_length",
            observation.observation_identity
        );
    }
    if observation
        .observed_block_number
        .is_some_and(|value| value < 0)
    {
        bail!(
            "manifest drift alert {} has negative observed_block_number",
            observation.observation_identity
        );
    }
    if observation.observed_block_number.is_some() != observation.observed_block_hash.is_some() {
        bail!(
            "manifest drift alert {} must include observed_block_number and observed_block_hash together",
            observation.observation_identity
        );
    }
    if observation.last_observed_at < observation.first_observed_at {
        bail!(
            "manifest drift alert {} last_observed_at is before first_observed_at",
            observation.observation_identity
        );
    }
    if observation
        .remediated_at
        .is_some_and(|value| value < observation.first_observed_at)
    {
        bail!(
            "manifest drift alert {} remediated_at is before first_observed_at",
            observation.observation_identity
        );
    }
    ensure_json_object(
        "manifest drift alert raw_fact_ref",
        &observation.raw_fact_ref,
    )?;
    ensure_json_object(
        "manifest drift alert expected_material",
        &observation.expected_material,
    )?;
    ensure_json_object(
        "manifest drift alert observed_material",
        &observation.observed_material,
    )?;
    ensure_json_object(
        "manifest drift alert watch_plan_metadata",
        &observation.watch_plan_metadata,
    )?;
    ensure_json_object(
        "manifest drift alert alert_metadata",
        &observation.alert_metadata,
    )?;
    if let Some(metadata) = &observation.remediation_metadata {
        ensure_json_object("manifest drift alert remediation_metadata", metadata)?;
    }

    match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            if observation.proxy_contract_instance_id.is_some() {
                bail!(
                    "manifest code-hash drift alert {} must not set proxy_contract_instance_id",
                    observation.observation_identity
                );
            }
            if observation.expected_code_hash.is_none()
                || observation.observed_code_hash.is_none()
                || observation.observed_canonicality_state.is_none()
            {
                bail!(
                    "manifest code-hash drift alert {} must include expected and observed code-hash material",
                    observation.observation_identity
                );
            }
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            if observation.proxy_contract_instance_id != Some(observation.contract_instance_id) {
                bail!(
                    "manifest proxy implementation alert {} must preserve the proxy contract_instance_id as the alert subject",
                    observation.observation_identity
                );
            }
        }
    }

    Ok(())
}

pub(super) fn manifest_alert_observation_create_from_rendered(
    observation: &ManifestDriftAlertObservation,
) -> Result<ManifestDriftAlertObservationCreate> {
    let lifecycle_status = ManifestDriftAlertLifecycleStatus::parse(
        observation
            .alert_state
            .get("alert_status")
            .and_then(Value::as_str)
            .unwrap_or(ManifestDriftAlertLifecycleStatus::Active.as_str()),
    )?;
    let chain_id = observation
        .chain_id
        .clone()
        .or_else(|| alert_state_string_owned(observation, "chain"))
        .context("manifest drift alert observation is missing chain_id")?;
    let source_manifest_id = observation.source_manifest_id.or_else(|| {
        observation
            .alert_state
            .get("source_manifest_id")
            .and_then(Value::as_i64)
    });
    let contract_instance_id = match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            parse_required_alert_uuid(observation, "contract_instance_id")?
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            parse_required_alert_uuid(observation, "proxy_contract_instance_id")?
        }
    };
    let proxy_contract_instance_id = match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => None,
        ManifestDriftAlertKind::ProxyImplementation => Some(contract_instance_id),
    };
    let observed_implementation_contract_instance_id =
        parse_optional_alert_uuid(observation, "observed_implementation_contract_instance_id")?
            .or_else(|| {
                parse_optional_alert_uuid(observation, "implementation_contract_instance_id")
                    .ok()
                    .flatten()
            });

    Ok(ManifestDriftAlertObservationCreate {
        observation_identity: observation.event_identity.clone(),
        alert_kind: observation.alert_kind,
        lifecycle_status,
        namespace: observation.namespace.clone(),
        source_family: observation.source_family.clone(),
        manifest_version: observation.manifest_version,
        source_manifest_id,
        chain_id,
        contract_instance_id,
        proxy_contract_instance_id,
        expected_implementation_contract_instance_id: parse_optional_alert_uuid(
            observation,
            "expected_implementation_contract_instance_id",
        )?,
        observed_implementation_contract_instance_id,
        discovery_edge_id: observation
            .alert_state
            .get("discovery_edge_id")
            .and_then(Value::as_i64)
            .or_else(|| {
                observation
                    .raw_fact_ref
                    .get("discovery_edge_id")
                    .and_then(Value::as_i64)
            }),
        expected_code_hash: normalize_optional_evm_b256(&alert_state_string_owned(
            observation,
            "expected_code_hash",
        )),
        observed_code_hash: normalize_optional_evm_b256(&alert_state_string_owned(
            observation,
            "observed_code_hash",
        )),
        observed_code_byte_length: observation
            .alert_state
            .get("observed_code_byte_length")
            .and_then(Value::as_i64),
        observed_block_number: observation.block_number.or_else(|| {
            observation
                .alert_state
                .get("observed_block_number")
                .and_then(Value::as_i64)
        }),
        observed_block_hash: normalize_optional_evm_b256(
            &observation
                .block_hash
                .clone()
                .or_else(|| alert_state_string_owned(observation, "observed_block_hash")),
        ),
        observed_canonicality_state: Some(observation.canonicality_state),
        raw_fact_ref: observation.raw_fact_ref.clone(),
        expected_material: json!({}),
        observed_material: json!({}),
        watch_plan_metadata: json!({}),
        alert_metadata: observation.alert_state.clone(),
        remediation_status: alert_state_string_owned(observation, "remediation_status"),
        remediation_metadata: observation
            .alert_state
            .get("remediation_metadata")
            .cloned()
            .or_else(|| observation.alert_state.get("remediation").cloned()),
        first_observed_at: observation.observed_at,
        last_observed_at: observation.observed_at,
        remediated_at: None,
    })
}

fn alert_state_string_owned(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Option<String> {
    observation
        .alert_state
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_required_alert_uuid(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Result<Uuid> {
    let value = alert_state_string_owned(observation, field)
        .with_context(|| format!("manifest drift alert observation is missing {field}"))?;
    Uuid::parse_str(&value)
        .with_context(|| format!("manifest drift alert observation has invalid {field}"))
}

fn parse_optional_alert_uuid(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Result<Option<Uuid>> {
    alert_state_string_owned(observation, field)
        .map(|value| {
            Uuid::parse_str(&value)
                .with_context(|| format!("manifest drift alert observation has invalid {field}"))
        })
        .transpose()
}

pub(super) fn ensure_existing_manifest_alert_matches_request(
    stored: &ManifestDriftAlertObservation,
    request: &ManifestDriftAlertObservationCreate,
) -> Result<()> {
    let expected_alert_state = manifest_alert_state_from_create(request)?;
    let expected_canonicality = request
        .observed_canonicality_state
        .unwrap_or(CanonicalityState::Observed);

    if stored.event_identity != request.observation_identity
        || stored.alert_kind != request.alert_kind
        || stored.namespace != request.namespace
        || stored.source_family != request.source_family
        || stored.manifest_version != request.manifest_version
        || stored.source_manifest_id != request.source_manifest_id
        || stored.chain_id.as_deref() != Some(request.chain_id.as_str())
        || stored.block_number != request.observed_block_number
        || stored.block_hash != request.observed_block_hash
        || stored.raw_fact_ref != request.raw_fact_ref
        || stored.canonicality_state != expected_canonicality
        || stored.alert_state != expected_alert_state
        || stored.observed_at != request.last_observed_at
    {
        bail!(
            "manifest drift alert observation {} already exists with different persisted material",
            request.observation_identity
        );
    }

    Ok(())
}
