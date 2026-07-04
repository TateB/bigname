use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, Row};

use super::{
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BaseNormalizedRederiveReplayTargetSnapshot, reverse_claim_derivation_kind,
    reverse_claim_source_families, subregistry_derivation_kinds, subregistry_source_families,
    unwrapped_authority_derivation_kind, unwrapped_authority_source_families,
};

pub(super) async fn ensure_canonical_raw_log_floor(pool: &PgPool) -> Result<()> {
    let floor = sqlx::query_scalar::<_, Option<i64>>(canonical_raw_log_floor_sql())
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .fetch_one(pool)
        .await
        .context("failed to validate Base retained canonical raw-log floor")?;
    ensure_canonical_raw_log_floor_matches(floor)
}

pub(super) async fn ensure_canonical_raw_log_floor_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let floor = sqlx::query_scalar::<_, Option<i64>>(canonical_raw_log_floor_sql())
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to validate Base retained canonical raw-log floor")?;
    ensure_canonical_raw_log_floor_matches(floor)
}

pub(super) async fn ensure_delete_scope_replay_active(
    pool: &PgPool,
    replay_target_block: i64,
) -> Result<()> {
    let rows = sqlx::query(inactive_delete_scope_pairs_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_all(pool)
        .await
        .context(
            "failed to validate Base delete-scope source families against active replay manifests",
        )?;
    ensure_inactive_delete_scope_pairs_empty(rows)
}

pub(super) async fn ensure_delete_scope_replay_active_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<()> {
    let rows = sqlx::query(inactive_delete_scope_pairs_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_all(&mut **transaction)
        .await
        .context(
            "failed to validate Base delete-scope source families against active replay manifests",
        )?;
    ensure_inactive_delete_scope_pairs_empty(rows)
}

pub(super) async fn load_active_replay_target_snapshot(
    pool: &PgPool,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveReplayTargetSnapshot>> {
    let rows = sqlx::query(active_replay_target_snapshot_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_source_families())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_source_families())
        .fetch_all(pool)
        .await
        .context("failed to load Base active replay target snapshot")?;
    replay_target_snapshot_from_rows(rows)
}

pub(super) async fn load_active_replay_target_snapshot_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveReplayTargetSnapshot>> {
    let rows = sqlx::query(active_replay_target_snapshot_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_source_families())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_source_families())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base active replay target snapshot")?;
    replay_target_snapshot_from_rows(rows)
}

pub(super) async fn ensure_no_affected_rows_above_raw_log_head(
    pool: &PgPool,
    canonical_raw_log_head: i64,
) -> Result<()> {
    let count = sqlx::query_scalar::<_, i64>(affected_rows_above_raw_log_head_sql())
        .bind(canonical_raw_log_head)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(pool)
        .await
        .context("failed to validate Base affected rows against retained raw-log head")?;
    ensure_no_rows_above_raw_log_head(canonical_raw_log_head, count)
}

pub(super) async fn ensure_no_affected_rows_above_raw_log_head_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    canonical_raw_log_head: i64,
) -> Result<()> {
    let count = sqlx::query_scalar::<_, i64>(affected_rows_above_raw_log_head_sql())
        .bind(canonical_raw_log_head)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to validate Base affected rows against retained raw-log head")?;
    ensure_no_rows_above_raw_log_head(canonical_raw_log_head, count)
}

fn ensure_inactive_delete_scope_pairs_empty(rows: Vec<sqlx::postgres::PgRow>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let missing = rows
        .into_iter()
        .map(|row| {
            let derivation_kind: String = row.get("derivation_kind");
            let source_family: String = row.get("source_family");
            let replay_adapter: Option<String> = row.get("replay_adapter");
            let row_count: i64 = row.get("row_count");
            let min_block_number: i64 = row.get("min_block_number");
            let max_block_number: i64 = row.get("max_block_number");
            format!(
                "{derivation_kind}/{source_family} adapter={} rows={row_count} blocks={min_block_number}..={max_block_number}",
                replay_adapter.unwrap_or_else(|| "<unmapped>".to_owned())
            )
        })
        .collect::<Vec<_>>();
    bail!(
        "Base normalized-event rederive delete scope contains rows current full-closure replay will not re-emit: {}",
        missing.join(", ")
    );
}

fn ensure_no_rows_above_raw_log_head(canonical_raw_log_head: i64, count: i64) -> Result<()> {
    ensure!(
        count == 0,
        "Base normalized-event rederive found {count} affected rows above canonical raw-log head {canonical_raw_log_head}; refusing to delete rows that cannot be re-derived from retained raw facts"
    );
    Ok(())
}

fn replay_target_snapshot_from_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<BaseNormalizedRederiveReplayTargetSnapshot>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveReplayTargetSnapshot {
                replay_adapter: row.try_get("replay_adapter")?,
                source_family: row.try_get("source_family")?,
                address: row.try_get("address")?,
                from_block: row.try_get("from_block")?,
                to_block: row.try_get("to_block")?,
            })
        })
        .collect()
}

fn ensure_canonical_raw_log_floor_matches(floor: Option<i64>) -> Result<()> {
    let Some(floor) = floor else {
        bail!(
            "Base normalized-event rederive cannot validate retained raw-log floor: no canonical raw logs for {}",
            BASE_NORMALIZED_REDERIVE_CHAIN_ID
        );
    };
    ensure!(
        floor == BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        "Base normalized-event rederive retained canonical raw-log floor {floor} does not match closure boundary {}; refusing because the raw-fact replay cursor could be widened below the delete scope",
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK
    );
    Ok(())
}

fn active_replay_target_snapshot_sql() -> &'static str {
    r#"
    WITH manifest_declared_targets AS (
        SELECT
            mv.chain,
            mv.source_family,
            LOWER(cia.address) AS address,
            COALESCE(
                CASE
                    WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END,
                17571485
            ) AS from_block,
            COALESCE(cia.active_to_block_number, $1) AS to_block
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'::manifest_rollout_status
          AND mv.chain = 'base-mainnet'
    ),
    watched_targets AS (
        SELECT chain, source_family, address, from_block, to_block
        FROM manifest_declared_targets

        UNION

        SELECT
            de.chain_id AS chain,
            COALESCE(target_mv.source_family, mv.source_family) AS source_family,
            LOWER(cia.address) AS address,
            COALESCE(
                CASE
                    WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                    ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
                END,
                17571485
            ) AS from_block,
            COALESCE(
                CASE
                    WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                    WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                    ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
                END,
                $1
            ) AS to_block
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        LEFT JOIN manifest_versions target_mv
          ON target_mv.rollout_status = 'active'::manifest_rollout_status
         AND target_mv.namespace = mv.namespace
         AND target_mv.chain = de.chain_id
         AND target_mv.deployment_epoch = mv.deployment_epoch
         AND target_mv.source_family = CASE
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                 THEN 'ens_v2_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                 THEN 'basenames_base_resolver'
             ELSE NULL
         END
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'::manifest_rollout_status
          AND de.chain_id = 'base-mainnet'
          AND de.deactivated_at IS NULL
          AND de.edge_kind <> 'migration'
          AND (
              de.edge_kind <> 'resolver'
              OR mv.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
              OR target_mv.manifest_id IS NOT NULL
          )
          AND (
              de.active_from_block_number IS NULL
              OR cia.active_to_block_number IS NULL
              OR de.active_from_block_number <= cia.active_to_block_number
          )
          AND (
              cia.active_from_block_number IS NULL
              OR de.active_to_block_number IS NULL
              OR cia.active_from_block_number <= de.active_to_block_number
          )
    ),
    adapter_targets AS (
        SELECT
            'ens_v1_reverse_claim'::TEXT AS replay_adapter,
            source_family,
            address,
            from_block,
            to_block
        FROM manifest_declared_targets
        WHERE chain = 'base-mainnet'
          AND source_family = ANY($2::TEXT[])

        UNION

        SELECT
            'ens_v1_subregistry_discovery'::TEXT AS replay_adapter,
            source_family,
            address,
            from_block,
            to_block
        FROM watched_targets
        WHERE chain = 'base-mainnet'
          AND source_family = ANY($3::TEXT[])

        UNION

        SELECT
            'ens_v1_unwrapped_authority'::TEXT AS replay_adapter,
            source_family,
            address,
            from_block,
            to_block
        FROM watched_targets
        WHERE chain = 'base-mainnet'
          AND source_family = ANY($4::TEXT[])
    )
    SELECT replay_adapter, source_family, address, from_block, to_block
    FROM adapter_targets
    WHERE from_block <= $1
      AND to_block >= 17571485
    ORDER BY replay_adapter, source_family, address, from_block, to_block
    "#
}

fn canonical_raw_log_floor_sql() -> &'static str {
    r#"
    SELECT MIN(raw_logs.block_number)::BIGINT
    FROM raw_logs
    JOIN chain_lineage lineage
      ON lineage.chain_id = raw_logs.chain_id
     AND lineage.block_hash = raw_logs.block_hash
    WHERE raw_logs.chain_id = $1
      AND raw_logs.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND lineage.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
    "#
}

fn inactive_delete_scope_pairs_sql() -> &'static str {
    r#"
    WITH scoped_events AS (
        SELECT
            normalized_event_id,
            derivation_kind,
            source_family,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            CASE
                WHEN derivation_kind = $2 AND source_family = ANY($3::TEXT[])
                    THEN 'ens_v1_reverse_claim'
                WHEN derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[])
                    THEN 'ens_v1_subregistry_discovery'
                WHEN derivation_kind = $6 AND source_family = ANY($7::TEXT[])
                    THEN 'ens_v1_unwrapped_authority'
                ELSE NULL
            END AS replay_adapter
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND block_number BETWEEN 17571485 AND $1
          AND block_hash IS NOT NULL
          AND (
              (derivation_kind = $2 AND source_family = ANY($3::TEXT[]))
              OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[]))
              OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))
          )
    ),
    manifest_declared_targets AS (
        SELECT
            mv.chain,
            mv.source_family,
            LOWER(cia.address) AS address,
            COALESCE(
                CASE
                    WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END,
                17571485
            ) AS from_block,
            COALESCE(cia.active_to_block_number, $1) AS to_block
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'::manifest_rollout_status
          AND mv.chain = 'base-mainnet'
    ),
    watched_targets AS (
        SELECT chain, source_family, address, from_block, to_block
        FROM manifest_declared_targets

        UNION

        SELECT
            de.chain_id AS chain,
            COALESCE(target_mv.source_family, mv.source_family) AS source_family,
            LOWER(cia.address) AS address,
            COALESCE(
                CASE
                    WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                    ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
                END,
                17571485
            ) AS from_block,
            COALESCE(
                CASE
                    WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                    WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                    ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
                END,
                $1
            ) AS to_block
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        LEFT JOIN manifest_versions target_mv
          ON target_mv.rollout_status = 'active'::manifest_rollout_status
         AND target_mv.namespace = mv.namespace
         AND target_mv.chain = de.chain_id
         AND target_mv.deployment_epoch = mv.deployment_epoch
         AND target_mv.source_family = CASE
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                 THEN 'ens_v2_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                 THEN 'basenames_base_resolver'
             ELSE NULL
         END
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'::manifest_rollout_status
          AND de.chain_id = 'base-mainnet'
          AND de.deactivated_at IS NULL
          AND de.edge_kind <> 'migration'
          AND (
              de.edge_kind <> 'resolver'
              OR mv.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
              OR target_mv.manifest_id IS NOT NULL
          )
          AND (
              de.active_from_block_number IS NULL
              OR cia.active_to_block_number IS NULL
              OR de.active_from_block_number <= cia.active_to_block_number
          )
          AND (
              cia.active_from_block_number IS NULL
              OR de.active_to_block_number IS NULL
              OR cia.active_from_block_number <= de.active_to_block_number
          )
    ),
    scoped_events_with_log AS (
        SELECT scoped_events.*, LOWER(raw_logs.emitting_address) AS emitting_address
        FROM scoped_events
        LEFT JOIN raw_logs
          ON raw_logs.chain_id = 'base-mainnet'
         AND raw_logs.block_hash = scoped_events.block_hash
         AND raw_logs.transaction_hash = scoped_events.transaction_hash
         AND raw_logs.log_index = scoped_events.log_index
    ),
    uncovered_events AS (
        SELECT *
        FROM scoped_events_with_log event
        WHERE NOT (
            (
                event.replay_adapter = 'ens_v1_reverse_claim'
                AND EXISTS (
                    SELECT 1
                    FROM manifest_declared_targets target
                    WHERE target.chain = 'base-mainnet'
                      AND target.source_family = event.source_family
                      AND target.from_block <= event.block_number
                      AND event.block_number <= target.to_block
                      AND (
                          event.log_index IS NULL
                          OR event.emitting_address IS NULL
                          OR target.address = event.emitting_address
                      )
                )
            )
            OR (
                event.replay_adapter IN (
                    'ens_v1_subregistry_discovery',
                    'ens_v1_unwrapped_authority'
                )
                AND EXISTS (
                    SELECT 1
                    FROM watched_targets target
                    WHERE target.chain = 'base-mainnet'
                      AND target.source_family = event.source_family
                      AND target.from_block <= event.block_number
                      AND event.block_number <= target.to_block
                      AND (
                          event.log_index IS NULL
                          OR event.emitting_address IS NULL
                          OR target.address = event.emitting_address
                      )
                )
            )
        )
    )
    SELECT
        derivation_kind,
        source_family,
        replay_adapter,
        COUNT(*)::BIGINT AS row_count,
        MIN(block_number)::BIGINT AS min_block_number,
        MAX(block_number)::BIGINT AS max_block_number
    FROM uncovered_events
    GROUP BY derivation_kind, source_family, replay_adapter
    ORDER BY derivation_kind, source_family
    "#
}

fn affected_rows_above_raw_log_head_sql() -> &'static str {
    r#"
    SELECT COUNT(*)::BIGINT
    FROM normalized_events
    WHERE chain_id = 'base-mainnet'
      AND block_number > $1
      AND block_number >= 17571485
      AND block_hash IS NOT NULL
      AND (
          (derivation_kind = $2 AND source_family = ANY($3::TEXT[]))
          OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[]))
          OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))
      )
    "#
}
