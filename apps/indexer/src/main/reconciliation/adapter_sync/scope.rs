use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};

use crate::ens_v1_resolver::{GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1};

pub(super) async fn load_live_adapter_source_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<Vec<(String, String, i64, i64)>> {
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_block_hashes = block_hashes.to_vec();
    unique_block_hashes.sort();
    unique_block_hashes.dedup();

    let (from_block, to_block, stored_count): (Option<i64>, Option<i64>, i64) = sqlx::query_as(
        r#"
        SELECT MIN(block_number), MAX(block_number), COUNT(*)::BIGINT
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain)
    .bind(&unique_block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw-block range for chain {chain} adapter sync"))?;

    let stored_count = usize::try_from(stored_count)
        .context("raw-block adapter sync range count does not fit in usize")?;
    if stored_count != unique_block_hashes.len() {
        bail!(
            "stored raw block count {stored_count} does not match adapter sync block-hash count {} for chain {chain}",
            unique_block_hashes.len()
        );
    }
    let (Some(from_block), Some(to_block)) = (from_block, to_block) else {
        bail!("adapter sync block range is empty for non-empty block-hash input on chain {chain}");
    };

    let raw_log_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(&unique_block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to count raw-log emitters for chain {chain} adapter sync range {from_block}..={to_block}"
        )
    })?;
    if raw_log_count == 0 {
        return Ok(Vec::new());
    }

    let scoped_rows = load_live_adapter_source_scope_rows(pool, chain, &unique_block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to load source-scoped watched contracts for chain {chain} adapter sync range {from_block}..={to_block}"
            )
        })?;

    Ok(collapse_live_adapter_source_scope(
        scoped_rows,
        from_block,
        to_block,
    ))
}

async fn load_live_adapter_source_scope_rows(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<Vec<(String, String, i64, i64)>> {
    sqlx::query_as(
        r#"
        WITH raw_log_targets AS (
            SELECT DISTINCT
                rl.chain_id AS chain,
                LOWER(rl.emitting_address) AS address,
                rl.block_number AS block_number
            FROM raw_logs rl
            WHERE rl.chain_id = $1
              AND rl.block_hash = ANY($2::TEXT[])
              AND rl.canonicality_state <> 'orphaned'::canonicality_state
        ),
        target_instances AS (
            SELECT
                raw.chain,
                raw.address,
                raw.block_number,
                cia.contract_instance_id
            FROM raw_log_targets raw
            JOIN contract_instance_addresses cia
              ON cia.chain_id = raw.chain
             AND cia.address = raw.address
             AND cia.deactivated_at IS NULL
             AND (
                 cia.active_from_block_number IS NULL
                 OR cia.active_from_block_number <= raw.block_number
             )
             AND (
                 cia.active_to_block_number IS NULL
                 OR raw.block_number <= cia.active_to_block_number
             )
        ),
        manifest_declared AS (
            SELECT DISTINCT
                ti.chain,
                mv.source_family,
                ti.address,
                ti.block_number AS effective_from_block,
                ti.block_number AS effective_to_block
            FROM target_instances ti
            JOIN manifest_contract_instances mci
              ON mci.contract_instance_id = ti.contract_instance_id
            JOIN manifest_versions mv
              ON mv.manifest_id = mci.manifest_id
             AND mv.chain = ti.chain
             AND mv.rollout_status = 'active'
            LEFT JOIN LATERAL (
                SELECT (entry ->> 'start_block')::BIGINT AS start_block
                FROM jsonb_array_elements(
                    CASE
                        WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                        ELSE mv.manifest_payload -> 'contracts'
                    END
                ) entry
                WHERE (
                        mci.declaration_kind = 'root'
                        AND entry ->> 'name' = mci.declaration_name
                    )
                   OR (
                        mci.declaration_kind = 'contract'
                        AND entry ->> 'role' = mci.declaration_name
                    )
                ORDER BY start_block NULLS LAST
                LIMIT 1
            ) manifest_range ON TRUE
            WHERE (
                manifest_range.start_block IS NULL
                OR manifest_range.start_block <= ti.block_number
            )
        ),
        direct_other_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                mv.source_family AS source_family
            FROM manifest_versions mv
            WHERE mv.rollout_status = 'active'
              AND mv.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
        ),
        direct_registry_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                mv.source_family AS source_family
            FROM manifest_versions mv
            WHERE mv.rollout_status = 'active'
              AND mv.source_family IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
        ),
        resolver_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                target_mv.source_family AS source_family
            FROM manifest_versions mv
            JOIN manifest_versions target_mv
              ON target_mv.rollout_status = 'active'
             AND target_mv.namespace = mv.namespace
             AND target_mv.chain = mv.chain
             AND target_mv.deployment_epoch = mv.deployment_epoch
             AND target_mv.source_family = CASE
                 WHEN mv.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
                 WHEN mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            WHERE mv.rollout_status = 'active'
              AND mv.source_family IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
        ),
        direct_other_discovery_scoped AS (
            SELECT DISTINCT
                ti.chain,
                candidate.source_family,
                ti.address,
                ti.block_number AS effective_from_block,
                ti.block_number AS effective_to_block
            FROM target_instances ti
            JOIN direct_other_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT 1
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND (
                      de.active_from_block_number IS NULL
                      OR de.active_from_block_number <= ti.block_number
                  )
                  AND (
                      de.active_to_block_number IS NULL
                      OR ti.block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        direct_registry_discovery_scoped AS (
            SELECT DISTINCT
                ti.chain,
                candidate.source_family,
                ti.address,
                ti.block_number AS effective_from_block,
                ti.block_number AS effective_to_block
            FROM target_instances ti
            JOIN direct_registry_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT 1
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND de.edge_kind <> 'resolver'
                  AND (
                      de.active_from_block_number IS NULL
                      OR de.active_from_block_number <= ti.block_number
                  )
                  AND (
                      de.active_to_block_number IS NULL
                      OR ti.block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        resolver_discovery_scoped AS (
            SELECT DISTINCT
                ti.chain,
                candidate.source_family,
                ti.address,
                ti.block_number AS effective_from_block,
                ti.block_number AS effective_to_block
            FROM target_instances ti
            JOIN resolver_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT 1
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind = 'resolver'
                  AND (
                      de.active_from_block_number IS NULL
                      OR de.active_from_block_number <= ti.block_number
                  )
                  AND (
                      de.active_to_block_number IS NULL
                      OR ti.block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        discovery_scoped AS (
            SELECT chain, source_family, address, effective_from_block, effective_to_block
            FROM direct_other_discovery_scoped

            UNION

            SELECT chain, source_family, address, effective_from_block, effective_to_block
            FROM direct_registry_discovery_scoped

            UNION

            SELECT chain, source_family, address, effective_from_block, effective_to_block
            FROM resolver_discovery_scoped
        )
        SELECT source_family, address, effective_from_block, effective_to_block
        FROM manifest_declared

        UNION

        SELECT source_family, address, effective_from_block, effective_to_block
        FROM discovery_scoped

        ORDER BY 1, 2, 3, 4
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .context("failed to load block-scoped live adapter source scope rows")
}

fn collapse_live_adapter_source_scope(
    rows: Vec<(String, String, i64, i64)>,
    from_block: i64,
    to_block: i64,
) -> Vec<(String, String, i64, i64)> {
    let include_generic_resolver_scope = rows
        .iter()
        .any(|(source_family, _, _, _)| source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1);
    let mut targets = BTreeSet::new();
    if include_generic_resolver_scope {
        targets.insert((
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
            GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
            from_block,
            to_block,
        ));
    }

    for (source_family, address, effective_from_block, effective_to_block) in rows {
        if include_generic_resolver_scope && source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
            continue;
        }
        targets.insert((
            source_family,
            address.to_ascii_lowercase(),
            effective_from_block,
            effective_to_block,
        ));
    }

    targets.into_iter().collect()
}
