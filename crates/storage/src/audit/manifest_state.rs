use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::CanonicalityState;

use super::{
    manifest_json::{
        insert_json, insert_optional_json, insert_uuid, json_object, merge_json_object,
    },
    types::{
        ManifestDriftAlertKind, ManifestDriftAlertLifecycleStatus,
        ManifestDriftAlertObservationCreate,
    },
};

pub(super) fn build_manifest_alert_state(
    alert_kind: ManifestDriftAlertKind,
    lifecycle_status: ManifestDriftAlertLifecycleStatus,
    row: &sqlx::postgres::PgRow,
    observed_canonicality_state: Option<CanonicalityState>,
) -> Result<Value> {
    let mut state = json_object(crate::sql_row::get(row, "alert_metadata")?)?;
    let expected_material: Value = crate::sql_row::get(row, "expected_material")?;
    let observed_material: Value = crate::sql_row::get(row, "observed_material")?;
    let watch_plan_metadata: Value = crate::sql_row::get(row, "watch_plan_metadata")?;

    insert_json(&mut state, "alert_type", alert_kind.alert_type());
    insert_json(&mut state, "alert_status", lifecycle_status.as_str());
    insert_json(
        &mut state,
        "source_family",
        crate::sql_row::get::<String>(row, "source_family")?,
    );
    insert_json(
        &mut state,
        "chain",
        crate::sql_row::get::<String>(row, "chain_id")?,
    );
    insert_optional_json(
        &mut state,
        "source_manifest_id",
        row.try_get::<Option<i64>, _>("source_manifest_id")
            .context("missing source_manifest_id")?,
    );
    insert_optional_json(
        &mut state,
        "remediation_status",
        row.try_get::<Option<String>, _>("remediation_status")
            .context("missing remediation_status")?,
    );
    insert_optional_json(
        &mut state,
        "remediation_metadata",
        row.try_get::<Option<Value>, _>("remediation_metadata")
            .context("missing remediation_metadata")?,
    );

    merge_json_object(&mut state, "expected_material", expected_material)?;
    merge_json_object(&mut state, "observed_material", observed_material)?;
    merge_json_object(&mut state, "watch_plan_metadata", watch_plan_metadata)?;

    match alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            insert_uuid(
                &mut state,
                "contract_instance_id",
                row.try_get::<Option<Uuid>, _>("contract_instance_id")
                    .context("missing contract_instance_id")?,
            );
            insert_optional_json(
                &mut state,
                "expected_code_hash",
                row.try_get::<Option<String>, _>("expected_code_hash")
                    .context("missing expected_code_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_code_hash",
                row.try_get::<Option<String>, _>("observed_code_hash")
                    .context("missing observed_code_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_code_byte_length",
                row.try_get::<Option<i64>, _>("observed_code_byte_length")
                    .context("missing observed_code_byte_length")?,
            );
            insert_optional_json(
                &mut state,
                "observed_block_number",
                row.try_get::<Option<i64>, _>("observed_block_number")
                    .context("missing observed_block_number")?,
            );
            insert_optional_json(
                &mut state,
                "observed_block_hash",
                row.try_get::<Option<String>, _>("observed_block_hash")
                    .context("missing observed_block_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_canonicality_state",
                observed_canonicality_state.map(CanonicalityState::as_str),
            );
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            insert_uuid(
                &mut state,
                "proxy_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("proxy_contract_instance_id")
                    .context("missing proxy_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "expected_implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("expected_implementation_contract_instance_id")
                    .context("missing expected_implementation_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "observed_implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("observed_implementation_contract_instance_id")
                    .context("missing observed_implementation_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("observed_implementation_contract_instance_id")
                    .context("missing observed_implementation_contract_instance_id")?,
            );
            insert_optional_json(
                &mut state,
                "discovery_edge_id",
                row.try_get::<Option<i64>, _>("discovery_edge_id")
                    .context("missing discovery_edge_id")?,
            );
        }
    }

    Ok(Value::Object(state))
}

pub(super) fn manifest_alert_state_from_create(
    observation: &ManifestDriftAlertObservationCreate,
) -> Result<Value> {
    let mut state = json_object(observation.alert_metadata.clone())?;

    insert_json(
        &mut state,
        "alert_type",
        observation.alert_kind.alert_type(),
    );
    insert_json(
        &mut state,
        "alert_status",
        observation.lifecycle_status.as_str(),
    );
    insert_json(
        &mut state,
        "source_family",
        observation.source_family.clone(),
    );
    insert_json(&mut state, "chain", observation.chain_id.clone());
    insert_optional_json(
        &mut state,
        "source_manifest_id",
        observation.source_manifest_id,
    );
    insert_optional_json(
        &mut state,
        "remediation_status",
        observation.remediation_status.clone(),
    );
    insert_optional_json(
        &mut state,
        "remediation_metadata",
        observation.remediation_metadata.clone(),
    );
    merge_json_object(
        &mut state,
        "expected_material",
        observation.expected_material.clone(),
    )?;
    merge_json_object(
        &mut state,
        "observed_material",
        observation.observed_material.clone(),
    )?;
    merge_json_object(
        &mut state,
        "watch_plan_metadata",
        observation.watch_plan_metadata.clone(),
    )?;

    match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            insert_json(
                &mut state,
                "contract_instance_id",
                observation.contract_instance_id.to_string(),
            );
            insert_optional_json(
                &mut state,
                "expected_code_hash",
                observation.expected_code_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_code_hash",
                observation.observed_code_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_code_byte_length",
                observation.observed_code_byte_length,
            );
            insert_optional_json(
                &mut state,
                "observed_block_number",
                observation.observed_block_number,
            );
            insert_optional_json(
                &mut state,
                "observed_block_hash",
                observation.observed_block_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_canonicality_state",
                observation
                    .observed_canonicality_state
                    .map(CanonicalityState::as_str),
            );
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            insert_json(
                &mut state,
                "proxy_contract_instance_id",
                observation.contract_instance_id.to_string(),
            );
            insert_optional_json(
                &mut state,
                "expected_implementation_contract_instance_id",
                observation
                    .expected_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "observed_implementation_contract_instance_id",
                observation
                    .observed_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "implementation_contract_instance_id",
                observation
                    .observed_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "discovery_edge_id",
                observation.discovery_edge_id,
            );
        }
    }

    Ok(Value::Object(state))
}
