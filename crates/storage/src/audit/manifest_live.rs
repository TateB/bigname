use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::evm_primitives::{
    normalize_evm_address, normalize_evm_b256, normalize_optional_evm_address,
};

use super::types::{
    MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE, MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND,
    ManifestDriftAlertKind,
};

pub(super) async fn load_live_code_hash_drift_candidates(pool: &PgPool) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        r#"
        WITH active_targets AS (
            SELECT
                mv.manifest_id,
                mv.manifest_version,
                mv.namespace,
                mv.source_family,
                mv.chain,
                mv.deployment_epoch,
                mci.declaration_kind,
                mci.declaration_name,
                mci.contract_instance_id,
                lower(mci.declared_address) AS declared_address,
                mci.code_hash AS expected_code_hash,
                CASE
                    WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                    ELSE 'manifest_contract'
                END::TEXT AS watched_source,
                cia.active_from_block_number,
                cia.active_to_block_number
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mci.code_hash IS NOT NULL
        ),
        latest_code AS (
            SELECT DISTINCT ON (
                active_targets.chain,
                active_targets.contract_instance_id,
                active_targets.declared_address
            )
                active_targets.*,
                raw_code_hashes.raw_code_hash_id,
                raw_code_hashes.block_hash AS observed_block_hash,
                raw_code_hashes.block_number AS observed_block_number,
                raw_code_hashes.code_hash AS observed_code_hash,
                raw_code_hashes.code_byte_length AS observed_code_byte_length,
                raw_code_hashes.canonicality_state::TEXT AS observed_canonicality_state,
                raw_code_hashes.observed_at AS raw_observed_at
            FROM active_targets
            JOIN raw_code_hashes
              ON raw_code_hashes.chain_id = active_targets.chain
             AND lower(raw_code_hashes.contract_address) = active_targets.declared_address
            WHERE raw_code_hashes.canonicality_state <> 'orphaned'
            ORDER BY
                active_targets.chain,
                active_targets.contract_instance_id,
                active_targets.declared_address,
                raw_code_hashes.block_number DESC,
                CASE raw_code_hashes.canonicality_state
                    WHEN 'finalized' THEN 4
                    WHEN 'safe' THEN 3
                    WHEN 'canonical' THEN 2
                    WHEN 'observed' THEN 1
                    ELSE 0
                END DESC,
                raw_code_hashes.raw_code_hash_id DESC
        )
        SELECT *
        FROM latest_code
        WHERE lower(observed_code_hash) <> lower(expected_code_hash)
        ORDER BY namespace, source_family, chain, declaration_kind, declaration_name, declared_address
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to compute live manifest code-hash drift audit candidates")?;

    rows.into_iter()
        .map(render_live_code_hash_drift_candidate)
        .collect()
}

pub(super) async fn load_live_proxy_implementation_candidates(pool: &PgPool) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mci.declaration_name,
            mci.role,
            mci.proxy_kind,
            mci.contract_instance_id AS proxy_contract_instance_id,
            lower(mci.declared_address) AS proxy_address,
            mci.implementation_contract_instance_id AS expected_implementation_contract_instance_id,
            lower(mci.declared_implementation_address) AS expected_implementation_address,
            de.discovery_edge_id,
            de.to_contract_instance_id AS observed_implementation_contract_instance_id,
            lower(implementation_address.address) AS observed_implementation_address,
            de.admission,
            de.active_from_block_number,
            de.active_to_block_number,
            de.provenance
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        LEFT JOIN discovery_edges de
          ON de.source_manifest_id = mv.manifest_id
         AND de.from_contract_instance_id = mci.contract_instance_id
         AND de.edge_kind = $1
         AND de.discovery_source = $2
         AND de.deactivated_at IS NULL
        LEFT JOIN contract_instance_addresses implementation_address
          ON implementation_address.contract_instance_id = de.to_contract_instance_id
         AND implementation_address.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
          AND mci.proxy_kind IS NOT NULL
          AND mci.proxy_kind <> 'none'
          AND mci.implementation_contract_instance_id IS NOT NULL
          AND (
              de.discovery_edge_id IS NULL
              OR de.to_contract_instance_id <> mci.implementation_contract_instance_id
          )
        ORDER BY mv.namespace, mv.source_family, mv.chain, mci.declaration_name, mci.declared_address
        "#,
    )
    .bind(MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND)
    .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
    .fetch_all(pool)
    .await
    .context("failed to compute live manifest proxy implementation audit candidates")?;

    rows.into_iter()
        .map(render_live_proxy_implementation_candidate)
        .collect()
}

fn render_live_code_hash_drift_candidate(row: sqlx::postgres::PgRow) -> Result<Value> {
    let contract_instance_id: Uuid = row
        .try_get("contract_instance_id")
        .context("missing live code-hash contract_instance_id")?;
    let manifest_id: i64 = row
        .try_get("manifest_id")
        .context("missing live code-hash manifest_id")?;
    let raw_code_hash_id: i64 = row
        .try_get("raw_code_hash_id")
        .context("missing live code-hash raw_code_hash_id")?;
    let raw_observed_at: OffsetDateTime = row
        .try_get("raw_observed_at")
        .context("missing live code-hash raw_observed_at")?;
    let declared_address = normalize_evm_address(
        &row.try_get::<String, _>("declared_address")
            .context("missing live code-hash declared_address")?,
    );
    let expected_code_hash = normalize_evm_b256(
        &row.try_get::<String, _>("expected_code_hash")
            .context("missing live code-hash expected_code_hash")?,
    );
    let observed_code_hash = normalize_evm_b256(
        &row.try_get::<String, _>("observed_code_hash")
            .context("missing live code-hash observed_code_hash")?,
    );
    let observed_block_hash = normalize_evm_b256(
        &row.try_get::<String, _>("observed_block_hash")
            .context("missing live code-hash observed_block_hash")?,
    );

    Ok(json!({
        "alert_type": ManifestDriftAlertKind::CodeHashDrift.alert_type(),
        "event_kind": ManifestDriftAlertKind::CodeHashDrift.event_kind(),
        "candidate_identity": format!(
            "live_manifest_drift:code_hash:{manifest_id}:{contract_instance_id}:{raw_code_hash_id}"
        ),
        "namespace": row.try_get::<String, _>("namespace").context("missing live code-hash namespace")?,
        "source_family": row.try_get::<String, _>("source_family").context("missing live code-hash source_family")?,
        "manifest_version": row.try_get::<i64, _>("manifest_version").context("missing live code-hash manifest_version")?,
        "source_manifest_id": manifest_id,
        "chain": row.try_get::<String, _>("chain").context("missing live code-hash chain")?,
        "deployment_epoch": row.try_get::<String, _>("deployment_epoch").context("missing live code-hash deployment_epoch")?,
        "lifecycle": {
            "status": "candidate",
            "active": true,
            "persisted": false,
        },
        "declaration": {
            "kind": row.try_get::<String, _>("declaration_kind").context("missing live code-hash declaration_kind")?,
            "name": row.try_get::<String, _>("declaration_name").context("missing live code-hash declaration_name")?,
        },
        "contract": {
            "contract_instance_id": contract_instance_id.to_string(),
            "address": declared_address,
        },
        "code_hash": {
            "expected": expected_code_hash,
            "observed": observed_code_hash,
            "observed_byte_length": row.try_get::<i64, _>("observed_code_byte_length").context("missing live code-hash observed_code_byte_length")?,
        },
        "observed_block": {
            "number": row.try_get::<i64, _>("observed_block_number").context("missing live code-hash observed_block_number")?,
            "hash": observed_block_hash,
            "canonicality_state": row.try_get::<String, _>("observed_canonicality_state").context("missing live code-hash observed_canonicality_state")?,
        },
        "watched_target": {
            "source": row.try_get::<String, _>("watched_source").context("missing live code-hash watched_source")?,
            "source_manifest_id": manifest_id,
            "active_block_range": {
                "from_block_number": row.try_get::<Option<i64>, _>("active_from_block_number").context("missing live code-hash active_from_block_number")?,
                "to_block_number": row.try_get::<Option<i64>, _>("active_to_block_number").context("missing live code-hash active_to_block_number")?,
            },
            "raw_fact_ref": {
                "raw_code_hash_id": raw_code_hash_id,
            },
        },
        "timestamps": {
            "observed_at": format_timestamp(raw_observed_at),
        },
        "remediation": Value::Null,
    }))
}

fn render_live_proxy_implementation_candidate(row: sqlx::postgres::PgRow) -> Result<Value> {
    let manifest_id: i64 = row
        .try_get("manifest_id")
        .context("missing live proxy manifest_id")?;
    let proxy_contract_instance_id: Uuid = row
        .try_get("proxy_contract_instance_id")
        .context("missing live proxy proxy_contract_instance_id")?;
    let expected_implementation_contract_instance_id: Uuid = row
        .try_get("expected_implementation_contract_instance_id")
        .context("missing live proxy expected_implementation_contract_instance_id")?;
    let observed_implementation_contract_instance_id: Option<Uuid> = row
        .try_get("observed_implementation_contract_instance_id")
        .context("missing live proxy observed_implementation_contract_instance_id")?;
    let discovery_edge_id: Option<i64> = row
        .try_get("discovery_edge_id")
        .context("missing live proxy discovery_edge_id")?;

    let candidate_reason = if discovery_edge_id.is_some() {
        "implementation_mismatch"
    } else {
        "missing_proxy_implementation_edge"
    };
    let proxy_address = normalize_evm_address(
        &row.try_get::<String, _>("proxy_address")
            .context("missing live proxy proxy_address")?,
    );
    let expected_implementation_address = normalize_optional_evm_address(
        &row.try_get::<Option<String>, _>("expected_implementation_address")
            .context("missing live proxy expected_implementation_address")?,
    );
    let observed_implementation_address = normalize_optional_evm_address(
        &row.try_get::<Option<String>, _>("observed_implementation_address")
            .context("missing live proxy observed_implementation_address")?,
    );

    Ok(json!({
        "alert_type": ManifestDriftAlertKind::ProxyImplementation.alert_type(),
        "event_kind": ManifestDriftAlertKind::ProxyImplementation.event_kind(),
        "candidate_identity": format!(
            "live_manifest_drift:proxy_implementation:{manifest_id}:{proxy_contract_instance_id}:{}",
            discovery_edge_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_owned())
        ),
        "candidate_reason": candidate_reason,
        "namespace": row.try_get::<String, _>("namespace").context("missing live proxy namespace")?,
        "source_family": row.try_get::<String, _>("source_family").context("missing live proxy source_family")?,
        "manifest_version": row.try_get::<i64, _>("manifest_version").context("missing live proxy manifest_version")?,
        "source_manifest_id": manifest_id,
        "chain": row.try_get::<String, _>("chain").context("missing live proxy chain")?,
        "lifecycle": {
            "status": "candidate",
            "active": true,
            "persisted": false,
        },
        "declaration": {
            "name": row.try_get::<String, _>("declaration_name").context("missing live proxy declaration_name")?,
            "role": row.try_get::<Option<String>, _>("role").context("missing live proxy role")?,
            "proxy_kind": row.try_get::<Option<String>, _>("proxy_kind").context("missing live proxy proxy_kind")?,
        },
        "proxy": {
            "contract_instance_id": proxy_contract_instance_id.to_string(),
            "address": proxy_address,
        },
        "expected_implementation": {
            "contract_instance_id": expected_implementation_contract_instance_id.to_string(),
            "address": expected_implementation_address,
        },
        "observed_implementation": {
            "contract_instance_id": observed_implementation_contract_instance_id
                .map(|value| value.to_string()),
            "address": observed_implementation_address,
        },
        "implementation_edge": {
            "discovery_edge_id": discovery_edge_id,
            "admission": row.try_get::<Option<String>, _>("admission").context("missing live proxy admission")?,
            "active_from_block_number": row.try_get::<Option<i64>, _>("active_from_block_number").context("missing live proxy active_from_block_number")?,
            "active_to_block_number": row.try_get::<Option<i64>, _>("active_to_block_number").context("missing live proxy active_to_block_number")?,
            "provenance": row.try_get::<Option<Value>, _>("provenance").context("missing live proxy provenance")?.unwrap_or(Value::Null),
        },
        "remediation": Value::Null,
    }))
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(sqlx::types::time::UtcOffset::UTC);
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
