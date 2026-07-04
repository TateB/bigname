use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaseNormalizedRederiveActiveManifestSnapshot {
    pub manifest_id: i64,
    pub manifest_version: i64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub file_path: String,
    pub manifest_payload: Value,
    pub capability_flags: Value,
    pub discovery_rules: Value,
    pub contract_instances: Value,
    pub discovery_edges: Value,
}

pub(super) async fn load_active_manifest_snapshot(
    pool: &PgPool,
) -> Result<Vec<BaseNormalizedRederiveActiveManifestSnapshot>> {
    let rows = sqlx::query(active_manifest_snapshot_sql())
        .fetch_all(pool)
        .await
        .context("failed to load Base active manifest snapshot")?;
    active_manifest_snapshot_from_rows(rows)
}

pub(super) async fn load_active_manifest_snapshot_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<BaseNormalizedRederiveActiveManifestSnapshot>> {
    let rows = sqlx::query(active_manifest_snapshot_sql())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base active manifest snapshot")?;
    active_manifest_snapshot_from_rows(rows)
}

fn active_manifest_snapshot_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<BaseNormalizedRederiveActiveManifestSnapshot>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveActiveManifestSnapshot {
                manifest_id: row.try_get("manifest_id")?,
                manifest_version: row.try_get("manifest_version")?,
                namespace: row.try_get("namespace")?,
                source_family: row.try_get("source_family")?,
                chain: row.try_get("chain")?,
                deployment_epoch: row.try_get("deployment_epoch")?,
                normalizer_version: row.try_get("normalizer_version")?,
                file_path: row.try_get("file_path")?,
                manifest_payload: row.try_get("manifest_payload")?,
                capability_flags: row.try_get("capability_flags")?,
                discovery_rules: row.try_get("discovery_rules")?,
                contract_instances: row.try_get("contract_instances")?,
                discovery_edges: row.try_get("discovery_edges")?,
            })
        })
        .collect()
}

fn active_manifest_snapshot_sql() -> &'static str {
    r#"
    SELECT
        mv.manifest_id,
        mv.manifest_version,
        mv.namespace,
        mv.source_family,
        mv.chain,
        mv.deployment_epoch,
        mv.normalizer_version,
        mv.file_path,
        mv.manifest_payload,
        COALESCE(capability_flags.rows, '[]'::jsonb) AS capability_flags,
        COALESCE(discovery_rules.rows, '[]'::jsonb) AS discovery_rules,
        COALESCE(contract_instances.rows, '[]'::jsonb) AS contract_instances,
        COALESCE(discovery_edges.rows, '[]'::jsonb) AS discovery_edges
    FROM manifest_versions mv
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'capability_name', mcf.capability_name,
                'status', mcf.status::text,
                'notes', mcf.notes
            )
            ORDER BY mcf.capability_name, mcf.status::text, mcf.notes
        ) AS rows
        FROM manifest_capability_flags mcf
        WHERE mcf.manifest_id = mv.manifest_id
    ) capability_flags ON TRUE
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'edge_kind', mdr.edge_kind,
                'from_role', mdr.from_role,
                'admission', mdr.admission,
                'rule_payload', mdr.rule_payload
            )
            ORDER BY mdr.edge_kind, mdr.from_role, mdr.admission, mdr.rule_payload::text
        ) AS rows
        FROM manifest_discovery_rules mdr
        WHERE mdr.manifest_id = mv.manifest_id
    ) discovery_rules ON TRUE
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'manifest_contract_instance_id', mci.manifest_contract_instance_id,
                'declaration_kind', mci.declaration_kind,
                'declaration_name', mci.declaration_name,
                'contract_instance_id', mci.contract_instance_id::text,
                'declared_address', lower(mci.declared_address),
                'code_hash', mci.code_hash,
                'abi_ref', mci.abi_ref,
                'role', mci.role,
                'proxy_kind', mci.proxy_kind,
                'implementation_contract_instance_id', mci.implementation_contract_instance_id::text,
                'declared_implementation_address', lower(mci.declared_implementation_address),
                'active_addresses', COALESCE(active_addresses.rows, '[]'::jsonb)
            )
            ORDER BY mci.declaration_kind,
                     mci.declaration_name,
                     lower(mci.declared_address),
                     mci.contract_instance_id::text,
                     mci.manifest_contract_instance_id
        ) AS rows
        FROM manifest_contract_instances mci
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'contract_instance_address_id', cia.contract_instance_address_id,
                    'chain_id', cia.chain_id,
                    'address', lower(cia.address),
                    'active_from_block_number', cia.active_from_block_number,
                    'active_from_block_hash', cia.active_from_block_hash,
                    'active_to_block_number', cia.active_to_block_number,
                    'active_to_block_hash', cia.active_to_block_hash,
                    'source_manifest_id', cia.source_manifest_id,
                    'provenance', cia.provenance
                )
                ORDER BY cia.chain_id,
                         lower(cia.address),
                         cia.active_from_block_number,
                         cia.active_to_block_number,
                         cia.source_manifest_id,
                         cia.contract_instance_address_id
            ) AS rows
            FROM contract_instance_addresses cia
            WHERE cia.contract_instance_id = mci.contract_instance_id
              AND cia.deactivated_at IS NULL
        ) active_addresses ON TRUE
        WHERE mci.manifest_id = mv.manifest_id
    ) contract_instances ON TRUE
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'discovery_edge_id', de.discovery_edge_id,
                'chain_id', de.chain_id,
                'edge_kind', de.edge_kind,
                'from_contract_instance_id', de.from_contract_instance_id::text,
                'to_contract_instance_id', de.to_contract_instance_id::text,
                'discovery_source', de.discovery_source,
                'source_manifest_id', de.source_manifest_id,
                'admission', de.admission,
                'active_from_block_number', de.active_from_block_number,
                'active_from_block_hash', de.active_from_block_hash,
                'active_to_block_number', de.active_to_block_number,
                'active_to_block_hash', de.active_to_block_hash,
                'provenance', de.provenance
            )
            ORDER BY de.chain_id,
                     de.edge_kind,
                     de.from_contract_instance_id::text,
                     de.to_contract_instance_id::text,
                     de.active_from_block_number,
                     de.active_to_block_number,
                     de.discovery_edge_id
        ) AS rows
        FROM discovery_edges de
        WHERE de.source_manifest_id = mv.manifest_id
          AND de.deactivated_at IS NULL
    ) discovery_edges ON TRUE
    WHERE mv.rollout_status = 'active'::manifest_rollout_status
      AND mv.chain = 'base-mainnet'
    ORDER BY mv.namespace,
             mv.source_family,
             mv.deployment_epoch,
             mv.manifest_version,
             mv.manifest_id
    "#
}
