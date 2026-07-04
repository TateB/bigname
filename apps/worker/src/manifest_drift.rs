use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, ManifestDriftAlertInspection, ManifestDriftAlertKind,
    ManifestDriftAlertObservation, normalize_evm_address, normalize_evm_b256,
};
use serde_json::{Value, json};
use sqlx::types::time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::cli::ManifestDriftAuditArgs;
use crate::inspect;

pub(crate) async fn audit(args: ManifestDriftAuditArgs) -> Result<()> {
    let _emit_json = args.json;
    let (pool, _rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &args.database,
            "bigname-worker",
        )
        .await?;
    let live_audit =
        bigname_storage::ManifestDriftAlertInspection::compute_live_manifest_drift_audit(&pool)
            .await?;
    let persisted = persist_manifest_drift_audit_observations(&pool, &live_audit).await?;
    let audit = render_manifest_drift_audit(&live_audit, &persisted);

    println!("{audit}");
    enforce_manifest_drift_audit_exit_policy(&persisted, args.fail_on_alert)?;
    Ok(())
}

async fn persist_manifest_drift_audit_observations(
    pool: &sqlx::PgPool,
    audit: &Value,
) -> Result<ManifestDriftAlertInspection> {
    for candidate in audit_alert_array(audit, "manifest_code_hash_drift_alerts")? {
        let observation = manifest_code_hash_drift_candidate_observation(candidate)?;
        ManifestDriftAlertInspection::persist_manifest_drift_alert_observation(pool, &observation)
            .await?;
    }

    for candidate in audit_alert_array(audit, "proxy_implementation_alerts")? {
        let observation = manifest_proxy_implementation_candidate_observation(
            candidate,
            manifest_drift_observed_at(),
        )?;
        ManifestDriftAlertInspection::persist_manifest_drift_alert_observation(pool, &observation)
            .await?;
    }

    bigname_storage::list_manifest_drift_alert_observations(pool).await
}

fn render_manifest_drift_audit(
    live_audit: &Value,
    persisted: &ManifestDriftAlertInspection,
) -> Value {
    let mut rendered =
        inspect::render_manifest_drift_alert_observations("manifest-drift audit", false, persisted);
    if let Some(object) = rendered.as_object_mut() {
        object.insert(
            "persistence".to_owned(),
            json!({
                "writes_normalized_events": false,
                "writes_alert_table": true,
                "mutates_manifest_truth": false,
                "mutates_discovery_edges": false,
                "mutates_watch_plan": false,
            }),
        );
        object.insert(
            "live_candidate_counts".to_owned(),
            live_audit
                .get("counts")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        object.insert(
            "actionable_persisted_alert_count".to_owned(),
            json!(manifest_drift_actionable_alert_count(persisted)),
        );
    }

    rendered
}

pub(crate) fn enforce_manifest_drift_audit_exit_policy(
    inspection: &ManifestDriftAlertInspection,
    fail_on_alert: bool,
) -> Result<()> {
    if !fail_on_alert {
        return Ok(());
    }

    let alert_count = manifest_drift_actionable_alert_count(inspection);
    if alert_count > 0 {
        bail!("manifest drift audit found {alert_count} actionable persisted alert(s)");
    }

    Ok(())
}

fn manifest_drift_actionable_alert_count(inspection: &ManifestDriftAlertInspection) -> usize {
    inspection
        .code_hash_drift_alerts
        .iter()
        .chain(inspection.proxy_implementation_alerts.iter())
        .filter(|alert| manifest_drift_alert_is_actionable(alert))
        .count()
}

fn manifest_drift_alert_is_actionable(alert: &ManifestDriftAlertObservation) -> bool {
    !matches!(
        alert
            .alert_state
            .get("alert_status")
            .and_then(Value::as_str),
        Some("dismissed" | "remediated")
    )
}

fn audit_alert_array<'a>(audit: &'a Value, field: &str) -> Result<&'a [Value]> {
    audit
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .with_context(|| format!("manifest drift audit JSON is missing {field}"))
}

fn manifest_code_hash_drift_candidate_observation(
    candidate: &Value,
) -> Result<ManifestDriftAlertObservation> {
    let declaration = required_object(candidate, "declaration")?;
    let contract = required_object(candidate, "contract")?;
    let code_hash = required_object(candidate, "code_hash")?;
    let observed_block = required_object(candidate, "observed_block")?;
    let watched_target = required_object(candidate, "watched_target")?;
    let timestamps = required_object(candidate, "timestamps")?;
    let chain = required_string(candidate, "chain")?;
    let source_family = required_string(candidate, "source_family")?;
    let source_manifest_id = required_i64(candidate, "source_manifest_id")?;
    let block_number = required_i64(observed_block, "number")?;
    let block_hash = normalize_evm_b256(required_string(observed_block, "hash")?);
    let contract_address = normalize_evm_address(required_string(contract, "address")?);
    let expected_code_hash = normalize_evm_b256(required_string(code_hash, "expected")?);
    let observed_code_hash = normalize_evm_b256(required_string(code_hash, "observed")?);
    let canonicality_state =
        parse_manifest_drift_canonicality(required_string(observed_block, "canonicality_state")?)?;
    let raw_fact_ref = required_value(watched_target, "raw_fact_ref")?.clone();
    ensure_json_object(&raw_fact_ref, "manifest drift code-hash raw_fact_ref")?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: 0,
        event_identity: required_string(candidate, "candidate_identity")?.to_owned(),
        alert_kind: ManifestDriftAlertKind::CodeHashDrift,
        namespace: required_string(candidate, "namespace")?.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: required_i64(candidate, "manifest_version")?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.clone()),
        raw_fact_ref,
        canonicality_state,
        alert_state: json!({
            "alert_status": "active",
            "declaration_kind": required_string(declaration, "kind")?,
            "declaration_name": required_string(declaration, "name")?,
            "contract_instance_id": required_string(contract, "contract_instance_id")?,
            "address": contract_address,
            "expected_code_hash": expected_code_hash,
            "observed_code_hash": observed_code_hash,
            "observed_code_byte_length": required_i64(code_hash, "observed_byte_length")?,
            "observed_block_number": block_number,
            "observed_block_hash": block_hash,
            "observed_canonicality_state": canonicality_state.as_str(),
            "watched_source": required_string(watched_target, "source")?,
            "source_manifest_id": source_manifest_id,
        }),
        observed_at: parse_manifest_drift_timestamp(required_string(timestamps, "observed_at")?)?,
    })
}

pub(crate) fn manifest_proxy_implementation_candidate_observation(
    candidate: &Value,
    observed_at: OffsetDateTime,
) -> Result<ManifestDriftAlertObservation> {
    let declaration = required_object(candidate, "declaration")?;
    let proxy = required_object(candidate, "proxy")?;
    let expected = required_object(candidate, "expected_implementation")?;
    let observed = required_object(candidate, "observed_implementation")?;
    let implementation_edge = required_object(candidate, "implementation_edge")?;
    let chain = required_string(candidate, "chain")?;
    let source_family = required_string(candidate, "source_family")?;
    let source_manifest_id = required_i64(candidate, "source_manifest_id")?;
    let discovery_edge_id = optional_i64(implementation_edge, "discovery_edge_id")?;
    let observed_implementation_contract_instance_id =
        optional_string(observed, "contract_instance_id")?;
    let proxy_address = normalize_evm_address(required_string(proxy, "address")?);
    let expected_implementation_address =
        optional_string(expected, "address")?.map(normalize_evm_address);
    let observed_implementation_address =
        optional_string(observed, "address")?.map(normalize_evm_address);

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: 0,
        event_identity: required_string(candidate, "candidate_identity")?.to_owned(),
        alert_kind: ManifestDriftAlertKind::ProxyImplementation,
        namespace: required_string(candidate, "namespace")?.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: required_i64(candidate, "manifest_version")?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.to_owned()),
        block_number: None,
        block_hash: None,
        raw_fact_ref: json!({
            "manifest_id": source_manifest_id,
            "discovery_edge_id": discovery_edge_id,
            "proxy_contract_instance_id": required_string(proxy, "contract_instance_id")?,
            "expected_implementation_contract_instance_id": required_string(expected, "contract_instance_id")?,
            "observed_implementation_contract_instance_id": observed_implementation_contract_instance_id,
        }),
        canonicality_state: CanonicalityState::Observed,
        alert_state: json!({
            "alert_status": "active",
            "candidate_reason": required_string(candidate, "candidate_reason")?,
            "declaration_name": required_string(declaration, "name")?,
            "role": optional_string(declaration, "role")?,
            "proxy_kind": optional_string(declaration, "proxy_kind")?,
            "proxy_contract_instance_id": required_string(proxy, "contract_instance_id")?,
            "proxy_address": proxy_address,
            "expected_implementation_contract_instance_id": required_string(expected, "contract_instance_id")?,
            "expected_implementation_address": expected_implementation_address,
            "observed_implementation_contract_instance_id": observed_implementation_contract_instance_id,
            "implementation_contract_instance_id": observed_implementation_contract_instance_id,
            "implementation_address": observed_implementation_address,
            "discovery_edge_id": discovery_edge_id,
            "admission": optional_string(implementation_edge, "admission")?,
            "active_from_block_number": optional_i64(implementation_edge, "active_from_block_number")?,
            "active_to_block_number": optional_i64(implementation_edge, "active_to_block_number")?,
            "provenance": required_value(implementation_edge, "provenance")?.clone(),
            "source_manifest_id": source_manifest_id,
        }),
        observed_at,
    })
}

fn required_value<'a>(object: &'a Value, field: &str) -> Result<&'a Value> {
    object
        .get(field)
        .with_context(|| format!("manifest drift candidate is missing {field}"))
}

fn required_object<'a>(object: &'a Value, field: &str) -> Result<&'a Value> {
    let value = required_value(object, field)?;
    ensure_json_object(value, field)?;
    Ok(value)
}

fn required_string<'a>(object: &'a Value, field: &str) -> Result<&'a str> {
    required_value(object, field)?
        .as_str()
        .with_context(|| format!("manifest drift candidate {field} must be a string"))
}

fn optional_string<'a>(object: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match object.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .with_context(|| format!("manifest drift candidate {field} must be a string or null")),
    }
}

fn required_i64(object: &Value, field: &str) -> Result<i64> {
    required_value(object, field)?
        .as_i64()
        .with_context(|| format!("manifest drift candidate {field} must be an integer"))
}

fn optional_i64(object: &Value, field: &str) -> Result<Option<i64>> {
    match object.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(value) => value.as_i64().map(Some).with_context(|| {
            format!("manifest drift candidate {field} must be an integer or null")
        }),
    }
}

fn ensure_json_object(value: &Value, context: &str) -> Result<()> {
    if !value.is_object() {
        bail!("{context} must be a JSON object");
    }
    Ok(())
}

fn parse_manifest_drift_canonicality(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown manifest drift canonicality_state value {value}"),
    }
}

fn parse_manifest_drift_timestamp(value: &str) -> Result<OffsetDateTime> {
    if value.len() != 20 || !value.ends_with('Z') {
        bail!("manifest drift timestamp {value} must use YYYY-MM-DDTHH:MM:SSZ");
    }

    let year = value[0..4]
        .parse::<i32>()
        .with_context(|| format!("invalid manifest drift timestamp year in {value}"))?;
    let month = value[5..7]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp month in {value}"))?;
    let day = value[8..10]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp day in {value}"))?;
    let hour = value[11..13]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp hour in {value}"))?;
    let minute = value[14..16]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp minute in {value}"))?;
    let second = value[17..19]
        .parse::<u8>()
        .with_context(|| format!("invalid manifest drift timestamp second in {value}"))?;
    if &value[4..5] != "-"
        || &value[7..8] != "-"
        || &value[10..11] != "T"
        || &value[13..14] != ":"
        || &value[16..17] != ":"
    {
        bail!("manifest drift timestamp {value} must use YYYY-MM-DDTHH:MM:SSZ");
    }

    let date = Date::from_ordinal_date(year, ordinal_day(year, month, day)?)
        .with_context(|| format!("invalid manifest drift timestamp date in {value}"))?;
    let time = Time::from_hms(hour, minute, second)
        .with_context(|| format!("invalid manifest drift timestamp time in {value}"))?;

    Ok(PrimitiveDateTime::new(date, time).assume_offset(UtcOffset::UTC))
}

fn manifest_drift_observed_at() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn ordinal_day(year: i32, month: u8, day: u8) -> Result<u16> {
    let leap_adjusted_days = [
        31_u16,
        if is_leap_year(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let month_index = usize::from(
        month
            .checked_sub(1)
            .context("manifest drift timestamp month must be in 1..=12")?,
    );
    let month_days = *leap_adjusted_days
        .get(month_index)
        .context("manifest drift timestamp month must be in 1..=12")?;
    if day == 0 || u16::from(day) > month_days {
        bail!("manifest drift timestamp day {day} is invalid for month {month}");
    }

    Ok(leap_adjusted_days[..month_index].iter().sum::<u16>() + u16::from(day))
}

const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}
