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
    WITH active_base_manifests AS (
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mv.normalizer_version,
            mv.file_path,
            mv.manifest_payload
        FROM manifest_versions mv
        WHERE mv.rollout_status = 'active'::manifest_rollout_status
          AND mv.chain = 'base-mainnet'
    ),
    manifest_snapshot_rows AS (
        SELECT
            mcf.manifest_id,
            'capability_flags'::TEXT AS collection_name,
            mcf.capability_name AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(mcf.capability_name, mcf.status::TEXT, mcf.notes)::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM manifest_capability_flags mcf
        JOIN active_base_manifests mv ON mv.manifest_id = mcf.manifest_id

        UNION ALL

        SELECT
            mdr.manifest_id,
            'discovery_rules'::TEXT AS collection_name,
            concat_ws('|', mdr.edge_kind, mdr.from_role, mdr.admission, mdr.rule_payload::TEXT)
                AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(
                        mdr.edge_kind,
                        mdr.from_role,
                        mdr.admission,
                        mdr.rule_payload
                    )::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM manifest_discovery_rules mdr
        JOIN active_base_manifests mv ON mv.manifest_id = mdr.manifest_id

        UNION ALL

        SELECT
            mci.manifest_id,
            'contract_instances'::TEXT AS collection_name,
            'manifest_contract_instance:' || mci.manifest_contract_instance_id::TEXT AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(
                        'manifest_contract_instance',
                        mci.manifest_contract_instance_id,
                        mci.declaration_kind,
                        mci.declaration_name,
                        mci.contract_instance_id::TEXT,
                        lower(mci.declared_address),
                        mci.code_hash,
                        mci.abi_ref,
                        mci.role,
                        mci.proxy_kind,
                        mci.implementation_contract_instance_id::TEXT,
                        lower(mci.declared_implementation_address)
                    )::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM manifest_contract_instances mci
        JOIN active_base_manifests mv ON mv.manifest_id = mci.manifest_id

        UNION ALL

        SELECT
            cia.source_manifest_id AS manifest_id,
            'contract_instances'::TEXT AS collection_name,
            'active_address:' || cia.contract_instance_address_id::TEXT AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(
                        'active_address',
                        cia.contract_instance_address_id,
                        cia.contract_instance_id::TEXT,
                        cia.chain_id,
                        lower(cia.address),
                        cia.active_from_block_number,
                        cia.active_from_block_hash,
                        cia.active_to_block_number,
                        cia.active_to_block_hash,
                        cia.source_manifest_id,
                        cia.provenance
                    )::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM contract_instance_addresses cia
        JOIN active_base_manifests mv ON mv.manifest_id = cia.source_manifest_id
        WHERE cia.deactivated_at IS NULL

        UNION ALL

        SELECT
            mci.manifest_id,
            'contract_instances'::TEXT AS collection_name,
            'active_address:' || cia.contract_instance_address_id::TEXT AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(
                        'active_address',
                        cia.contract_instance_address_id,
                        cia.contract_instance_id::TEXT,
                        cia.chain_id,
                        lower(cia.address),
                        cia.active_from_block_number,
                        cia.active_from_block_hash,
                        cia.active_to_block_number,
                        cia.active_to_block_hash,
                        cia.source_manifest_id,
                        cia.provenance
                    )::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM manifest_contract_instances mci
        JOIN active_base_manifests mv ON mv.manifest_id = mci.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
        WHERE cia.deactivated_at IS NULL
          AND cia.source_manifest_id IS DISTINCT FROM mci.manifest_id

        UNION ALL

        SELECT
            de.source_manifest_id AS manifest_id,
            'discovery_edges'::TEXT AS collection_name,
            de.discovery_edge_id::TEXT AS row_key,
            encode(
                sha256(convert_to(
                    jsonb_build_array(
                        de.discovery_edge_id,
                        de.chain_id,
                        de.edge_kind,
                        de.from_contract_instance_id::TEXT,
                        de.to_contract_instance_id::TEXT,
                        de.discovery_source,
                        de.source_manifest_id,
                        de.admission,
                        de.active_from_block_number,
                        de.active_from_block_hash,
                        de.active_to_block_number,
                        de.active_to_block_hash,
                        de.provenance
                    )::TEXT,
                    'UTF8'
                )),
                'hex'
            ) AS row_hash
        FROM discovery_edges de
        JOIN active_base_manifests mv ON mv.manifest_id = de.source_manifest_id
        WHERE de.deactivated_at IS NULL
    ),
    manifest_snapshot_row_hash_limbs AS (
        SELECT
            manifest_id,
            collection_name,
            row_key,
            (('x' || substr(row_hash, 1, 8))::BIT(32)::BIGINT) AS limb_0,
            (('x' || substr(row_hash, 9, 8))::BIT(32)::BIGINT) AS limb_1,
            (('x' || substr(row_hash, 17, 8))::BIT(32)::BIGINT) AS limb_2,
            (('x' || substr(row_hash, 25, 8))::BIT(32)::BIGINT) AS limb_3,
            (('x' || substr(row_hash, 33, 8))::BIT(32)::BIGINT) AS limb_4,
            (('x' || substr(row_hash, 41, 8))::BIT(32)::BIGINT) AS limb_5,
            (('x' || substr(row_hash, 49, 8))::BIT(32)::BIGINT) AS limb_6,
            (('x' || substr(row_hash, 57, 8))::BIT(32)::BIGINT) AS limb_7
        FROM manifest_snapshot_rows
    ),
    manifest_snapshot_collection_summaries AS (
        SELECT
            manifest_id,
            collection_name,
            jsonb_build_object(
                'digest_kind', 'base_active_manifest_collection_digest_v1',
                'hash_algorithm', 'sha256',
                'combine', 'count-min-max-sum-xor-u32-limbs',
                'collection', collection_name,
                'row_count', COUNT(*)::BIGINT,
                'row_key_min', MIN(row_key),
                'row_key_max', MAX(row_key),
                'sha256_limb_sums', jsonb_build_array(
                    COALESCE(SUM(limb_0::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_1::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_2::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_3::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_4::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_5::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_6::NUMERIC), 0)::TEXT,
                    COALESCE(SUM(limb_7::NUMERIC), 0)::TEXT
                ),
                'sha256_limb_xors', jsonb_build_array(
                    COALESCE(bit_xor(limb_0), 0)::TEXT,
                    COALESCE(bit_xor(limb_1), 0)::TEXT,
                    COALESCE(bit_xor(limb_2), 0)::TEXT,
                    COALESCE(bit_xor(limb_3), 0)::TEXT,
                    COALESCE(bit_xor(limb_4), 0)::TEXT,
                    COALESCE(bit_xor(limb_5), 0)::TEXT,
                    COALESCE(bit_xor(limb_6), 0)::TEXT,
                    COALESCE(bit_xor(limb_7), 0)::TEXT
                )
            ) AS rows
        FROM manifest_snapshot_row_hash_limbs
        GROUP BY manifest_id, collection_name
    ),
    manifest_snapshot_empty_collections AS (
        SELECT mv.manifest_id, collection.collection_name
        FROM active_base_manifests mv
        CROSS JOIN (
            VALUES
                ('capability_flags'::TEXT),
                ('discovery_rules'::TEXT),
                ('contract_instances'::TEXT),
                ('discovery_edges'::TEXT)
        ) AS collection(collection_name)
    ),
    manifest_snapshot_summaries AS (
        SELECT
            empty.manifest_id,
            empty.collection_name,
            COALESCE(summary.rows, jsonb_build_object(
                'digest_kind', 'base_active_manifest_collection_digest_v1',
                'hash_algorithm', 'sha256',
                'combine', 'count-min-max-sum-xor-u32-limbs',
                'collection', empty.collection_name,
                'row_count', 0,
                'row_key_min', NULL,
                'row_key_max', NULL,
                'sha256_limb_sums',
                    jsonb_build_array('0', '0', '0', '0', '0', '0', '0', '0'),
                'sha256_limb_xors',
                    jsonb_build_array('0', '0', '0', '0', '0', '0', '0', '0')
            )) AS rows
        FROM manifest_snapshot_empty_collections empty
        LEFT JOIN manifest_snapshot_collection_summaries summary
          ON summary.manifest_id = empty.manifest_id
         AND summary.collection_name = empty.collection_name
    )
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
        capability_flags.rows AS capability_flags,
        discovery_rules.rows AS discovery_rules,
        contract_instances.rows AS contract_instances,
        discovery_edges.rows AS discovery_edges
    FROM active_base_manifests mv
    JOIN manifest_snapshot_summaries capability_flags
      ON capability_flags.manifest_id = mv.manifest_id
     AND capability_flags.collection_name = 'capability_flags'
    JOIN manifest_snapshot_summaries discovery_rules
      ON discovery_rules.manifest_id = mv.manifest_id
     AND discovery_rules.collection_name = 'discovery_rules'
    JOIN manifest_snapshot_summaries contract_instances
      ON contract_instances.manifest_id = mv.manifest_id
     AND contract_instances.collection_name = 'contract_instances'
    JOIN manifest_snapshot_summaries discovery_edges
      ON discovery_edges.manifest_id = mv.manifest_id
     AND discovery_edges.collection_name = 'discovery_edges'
    ORDER BY mv.namespace,
             mv.source_family,
             mv.deployment_epoch,
             mv.manifest_version,
             mv.manifest_id
    "#
}
