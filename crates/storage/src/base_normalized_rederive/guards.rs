use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgConnection, PgPool, Row};

use super::{
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BaseNormalizedRederiveRatifiedDroppedEmitterCensus, BaseNormalizedRederiveReplayTargetSnapshot,
    reverse_claim_derivation_kind, reverse_claim_source_families, subregistry_derivation_kinds,
    subregistry_source_families, unwrapped_authority_derivation_kind,
    unwrapped_authority_source_families,
};

mod emitter;
mod pairs;

use emitter::ensure_delete_scope_emitters_replay_active_from;
use pairs::ensure_delete_scope_pairs_replay_active_from;

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

#[cfg(test)]
pub(super) async fn ensure_delete_scope_replay_active(
    pool: &PgPool,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open Base active replay target guard transaction")?;
    ensure_delete_scope_replay_active_from(
        &mut transaction,
        replay_target_block,
        active_replay_target_snapshot,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to close Base active replay target guard transaction")?;
    Ok(())
}

pub(super) async fn ensure_delete_scope_replay_active_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
    active_replay_target_snapshot: &[BaseNormalizedRederiveReplayTargetSnapshot],
) -> Result<()> {
    let _ = active_replay_target_snapshot;
    ensure_active_replay_target_snapshot_table(transaction, replay_target_block).await?;
    ensure_delete_scope_pairs_replay_active_from(transaction, replay_target_block).await?;
    ensure_delete_scope_emitters_replay_active_from(transaction, replay_target_block).await
}

pub(super) async fn load_ratified_dropped_orphan_emitter_census_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveRatifiedDroppedEmitterCensus>> {
    emitter::load_ratified_dropped_orphan_emitter_census_from(transaction, replay_target_block)
        .await
}

pub(super) async fn load_active_replay_target_snapshot(
    pool: &PgPool,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveReplayTargetSnapshot>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open Base active replay target snapshot transaction")?;
    let snapshot =
        load_active_replay_target_snapshot_from(&mut transaction, replay_target_block).await?;
    transaction
        .commit()
        .await
        .context("failed to close Base active replay target snapshot transaction")?;
    Ok(snapshot)
}

pub(super) async fn load_active_replay_target_snapshot_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveReplayTargetSnapshot>> {
    ensure_active_replay_target_snapshot_table(transaction, replay_target_block).await?;
    let rows = sqlx::query(active_replay_target_snapshot_table_sql())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base active replay target snapshot")?;
    replay_target_snapshot_from_rows(rows)
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

async fn ensure_active_replay_target_snapshot_table(
    connection: &mut PgConnection,
    replay_target_block: i64,
) -> Result<()> {
    let meta_table: Option<String> = sqlx::query_scalar(
        "SELECT to_regclass('pg_temp.base_rederive_active_replay_target_snapshot_meta')::TEXT",
    )
    .fetch_one(&mut *connection)
    .await
    .context("failed to inspect Base active replay target snapshot temp table")?;
    if meta_table.is_some() {
        let existing_target = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT replay_target_block
            FROM pg_temp.base_rederive_active_replay_target_snapshot_meta
            LIMIT 1
            "#,
        )
        .fetch_one(&mut *connection)
        .await
        .context("failed to inspect Base active replay target snapshot temp metadata")?;
        if existing_target == Some(replay_target_block) {
            return Ok(());
        }
    }

    drop_active_replay_target_snapshot_table(connection).await?;
    execute(
        connection,
        r#"
        CREATE TEMP TABLE base_rederive_active_replay_targets (
            replay_adapter TEXT NOT NULL,
            source_family TEXT NOT NULL,
            address TEXT NOT NULL,
            from_block BIGINT NOT NULL,
            to_block BIGINT NOT NULL
        ) ON COMMIT DROP
        "#,
    )
    .await?;

    // The guards join this temp table instead of rebinding the reviewed snapshot;
    // base-mainnet discovery targets can exceed one million rows.
    let insert_sql = active_replay_target_snapshot_insert_sql();
    sqlx::query(&insert_sql)
        .bind(replay_target_block)
        .bind(reverse_claim_source_families())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_source_families())
        .execute(&mut *connection)
        .await
        .context("failed to materialize Base active replay target snapshot temp table")?;

    execute(
        connection,
        r#"
        CREATE INDEX base_rederive_active_replay_targets_scope_idx
        ON base_rederive_active_replay_targets (
            source_family,
            address,
            from_block,
            to_block
        )
        "#,
    )
    .await?;
    execute(
        connection,
        r#"
        CREATE INDEX base_rederive_active_replay_targets_pair_idx
        ON base_rederive_active_replay_targets (
            replay_adapter,
            source_family,
            from_block,
            to_block
        )
        "#,
    )
    .await?;
    execute(connection, "ANALYZE base_rederive_active_replay_targets").await?;
    execute(
        connection,
        r#"
        CREATE TEMP TABLE base_rederive_active_replay_target_snapshot_meta (
            replay_target_block BIGINT NOT NULL
        ) ON COMMIT DROP
        "#,
    )
    .await?;
    sqlx::query(
        "INSERT INTO base_rederive_active_replay_target_snapshot_meta (replay_target_block) VALUES ($1)",
    )
    .bind(replay_target_block)
    .execute(&mut *connection)
    .await
    .context("failed to record Base active replay target snapshot temp metadata")?;
    Ok(())
}

async fn drop_active_replay_target_snapshot_table(connection: &mut PgConnection) -> Result<()> {
    execute(
        connection,
        "DROP TABLE IF EXISTS pg_temp.base_rederive_active_replay_target_snapshot_meta",
    )
    .await?;
    execute(
        connection,
        "DROP TABLE IF EXISTS pg_temp.base_rederive_active_replay_targets",
    )
    .await
}

async fn execute(connection: &mut PgConnection, sql: &str) -> Result<()> {
    sqlx::query(sql)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to execute Base active replay target SQL: {sql}"))?;
    Ok(())
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

fn active_replay_target_snapshot_insert_sql() -> String {
    format!(
        r#"
        INSERT INTO base_rederive_active_replay_targets (
            replay_adapter,
            source_family,
            address,
            from_block,
            to_block
        )
        {}
        "#,
        active_replay_target_snapshot_sql(false)
    )
}

fn active_replay_target_snapshot_table_sql() -> &'static str {
    r#"
    SELECT replay_adapter, source_family, address, from_block, to_block
    FROM base_rederive_active_replay_targets
    ORDER BY replay_adapter, source_family, address, from_block, to_block
    "#
}

fn active_replay_target_snapshot_sql(ordered: bool) -> String {
    let order_by = if ordered {
        "ORDER BY replay_adapter, source_family, address, from_block, to_block"
    } else {
        ""
    };
    format!(
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
    {order_by}
    "#
    )
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

#[cfg(test)]
pub(super) fn inactive_delete_scope_pairs_sql() -> &'static str {
    pairs::inactive_delete_scope_pairs_sql()
}

#[cfg(test)]
pub(super) fn orphaned_delete_scope_emitters_sql() -> String {
    emitter::orphaned_delete_scope_emitters_sql()
}

#[cfg(test)]
pub(super) fn ratified_dropped_orphan_emitter_census_sql() -> String {
    emitter::ratified_dropped_orphan_emitter_census_sql()
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
