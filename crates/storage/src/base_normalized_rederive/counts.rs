use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::{
    BaseNormalizedRederiveCounts, BaseNormalizedRederiveFamilyCensus,
    BaseNormalizedRederiveRawFactCompleteness, checkpoint_adapters, expected_manifest_ids,
};

pub(super) async fn load_counts(
    pool: &PgPool,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederiveCounts> {
    let row = sqlx::query(counts_sql())
        .bind(expected_manifest_ids())
        .bind(deployment_profile)
        .bind(checkpoint_adapters())
        .fetch_one(pool)
        .await
        .context("failed to load Base normalized-event rederive census")?;
    counts_from_row(&row)
}

pub(super) async fn load_counts_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederiveCounts> {
    let row = sqlx::query(counts_sql())
        .bind(expected_manifest_ids())
        .bind(deployment_profile)
        .bind(checkpoint_adapters())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive census")?;
    counts_from_row(&row)
}

pub(super) async fn load_family_census(
    pool: &PgPool,
) -> Result<Vec<BaseNormalizedRederiveFamilyCensus>> {
    let rows = sqlx::query(family_census_sql())
        .bind(expected_manifest_ids())
        .fetch_all(pool)
        .await
        .context("failed to load Base normalized-event rederive family census")?;
    family_census_rows(rows)
}

pub(super) async fn load_family_census_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Vec<BaseNormalizedRederiveFamilyCensus>> {
    let rows = sqlx::query(family_census_sql())
        .bind(expected_manifest_ids())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive family census")?;
    family_census_rows(rows)
}

pub(super) async fn load_raw_fact_completeness(
    pool: &PgPool,
) -> Result<BaseNormalizedRederiveRawFactCompleteness> {
    let row = sqlx::query(raw_fact_completeness_sql())
        .bind(expected_manifest_ids())
        .fetch_one(pool)
        .await
        .context("failed to load Base normalized-event rederive raw-fact completeness")?;
    raw_fact_completeness_from_row(&row)
}

pub(super) async fn load_raw_fact_completeness_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<BaseNormalizedRederiveRawFactCompleteness> {
    let row = sqlx::query(raw_fact_completeness_sql())
        .bind(expected_manifest_ids())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive raw-fact completeness")?;
    raw_fact_completeness_from_row(&row)
}

fn counts_sql() -> &'static str {
    r#"
    WITH
    scoped_events AS (
        SELECT normalized_event_id
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND source_manifest_id = ANY($1::BIGINT[])
          AND block_number BETWEEN 17571485 AND 46954147
          AND block_hash IS NOT NULL
          AND derivation_kind NOT IN ('manifest_sync', 'manifest_alert')
    ),
    scoped_resources AS (
        SELECT resource_id
        FROM resources
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
    ),
    scoped_token_lineages AS (
        SELECT token_lineage_id
        FROM token_lineages
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
    ),
    scoped_name_surfaces AS (
        SELECT logical_name_id
        FROM name_surfaces
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
    ),
    scoped_surface_bindings AS (
        SELECT surface_binding_id
        FROM surface_bindings
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
    )
    SELECT
        (SELECT COUNT(*)::BIGINT FROM scoped_events) AS normalized_events,
        (SELECT COUNT(*)::BIGINT FROM scoped_resources) AS resources,
        (SELECT COUNT(*)::BIGINT FROM scoped_token_lineages) AS token_lineages,
        (SELECT COUNT(*)::BIGINT FROM scoped_name_surfaces) AS name_surfaces,
        (SELECT COUNT(*)::BIGINT FROM scoped_surface_bindings) AS surface_bindings,
        (
            SELECT COUNT(*)::BIGINT
            FROM name_current p
            WHERE EXISTS (SELECT 1 FROM scoped_resources s WHERE s.resource_id = p.resource_id)
               OR EXISTS (SELECT 1 FROM scoped_token_lineages s WHERE s.token_lineage_id = p.token_lineage_id)
               OR EXISTS (SELECT 1 FROM scoped_name_surfaces s WHERE s.logical_name_id = p.logical_name_id)
               OR EXISTS (SELECT 1 FROM scoped_surface_bindings s WHERE s.surface_binding_id = p.surface_binding_id)
        ) AS name_current,
        (
            SELECT COUNT(*)::BIGINT
            FROM address_names_current p
            WHERE EXISTS (SELECT 1 FROM scoped_resources s WHERE s.resource_id = p.resource_id)
               OR EXISTS (SELECT 1 FROM scoped_token_lineages s WHERE s.token_lineage_id = p.token_lineage_id)
               OR EXISTS (SELECT 1 FROM scoped_name_surfaces s WHERE s.logical_name_id = p.logical_name_id)
               OR EXISTS (SELECT 1 FROM scoped_surface_bindings s WHERE s.surface_binding_id = p.surface_binding_id)
        ) AS address_names_current,
        (SELECT COUNT(*)::BIGINT FROM children_current p WHERE EXISTS (SELECT 1 FROM scoped_name_surfaces s WHERE s.logical_name_id IN (p.parent_logical_name_id, p.child_logical_name_id))) AS children_current,
        (SELECT COUNT(*)::BIGINT FROM permissions_current p WHERE EXISTS (SELECT 1 FROM scoped_resources s WHERE s.resource_id = p.resource_id)) AS permissions_current,
        (SELECT COUNT(*)::BIGINT FROM record_inventory_current p WHERE EXISTS (SELECT 1 FROM scoped_resources s WHERE s.resource_id = p.resource_id)) AS record_inventory_current,
        (SELECT COUNT(*)::BIGINT FROM projection_normalized_event_changes p WHERE EXISTS (SELECT 1 FROM scoped_events s WHERE s.normalized_event_id = p.normalized_event_id)) AS projection_normalized_event_changes,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_cursors WHERE deployment_profile = $2 AND chain_id = 'base-mainnet' AND cursor_kind = 'raw_fact_normalized_events') AS replay_cursor_rows,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoints WHERE deployment_profile = $2 AND chain_id = 'base-mainnet' AND cursor_kind = 'raw_fact_normalized_events' AND adapter = ANY($3::TEXT[])) AS adapter_checkpoint_rows,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoint_items WHERE deployment_profile = $2 AND chain_id = 'base-mainnet' AND cursor_kind = 'raw_fact_normalized_events' AND adapter = ANY($3::TEXT[])) AS adapter_checkpoint_item_rows
    FROM (SELECT 1) AS one
    "#
}

fn counts_from_row(row: &sqlx::postgres::PgRow) -> Result<BaseNormalizedRederiveCounts> {
    Ok(BaseNormalizedRederiveCounts {
        normalized_events: row.try_get("normalized_events")?,
        resources: row.try_get("resources")?,
        token_lineages: row.try_get("token_lineages")?,
        name_surfaces: row.try_get("name_surfaces")?,
        surface_bindings: row.try_get("surface_bindings")?,
        name_current: row.try_get("name_current")?,
        address_names_current: row.try_get("address_names_current")?,
        children_current: row.try_get("children_current")?,
        permissions_current: row.try_get("permissions_current")?,
        record_inventory_current: row.try_get("record_inventory_current")?,
        projection_normalized_event_changes: row.try_get("projection_normalized_event_changes")?,
        replay_cursor_rows: row.try_get("replay_cursor_rows")?,
        adapter_checkpoint_rows: row.try_get("adapter_checkpoint_rows")?,
        adapter_checkpoint_item_rows: row.try_get("adapter_checkpoint_item_rows")?,
    })
}

fn family_census_sql() -> &'static str {
    r#"
    SELECT
        mv.manifest_id AS source_manifest_id,
        mv.source_family,
        COUNT(ne.normalized_event_id)::BIGINT AS row_count,
        MIN(ne.block_number)::BIGINT AS min_block_number,
        MAX(ne.block_number)::BIGINT AS max_block_number
    FROM manifest_versions mv
    LEFT JOIN normalized_events ne
      ON ne.source_manifest_id = mv.manifest_id
     AND ne.chain_id = 'base-mainnet'
     AND ne.block_number BETWEEN 17571485 AND 46954147
     AND ne.block_hash IS NOT NULL
     AND ne.derivation_kind NOT IN ('manifest_sync', 'manifest_alert')
    WHERE mv.manifest_id = ANY($1::BIGINT[])
    GROUP BY mv.manifest_id, mv.source_family
    ORDER BY mv.manifest_id
    "#
}

fn family_census_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<BaseNormalizedRederiveFamilyCensus>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveFamilyCensus {
                source_manifest_id: row.try_get("source_manifest_id")?,
                source_family: row.try_get("source_family")?,
                row_count: row.try_get("row_count")?,
                min_block_number: row.try_get("min_block_number")?,
                max_block_number: row.try_get("max_block_number")?,
            })
        })
        .collect()
}

fn raw_fact_completeness_sql() -> &'static str {
    r#"
    WITH scoped_events AS (
        SELECT *
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND source_manifest_id = ANY($1::BIGINT[])
          AND block_number BETWEEN 17571485 AND 46954147
          AND block_hash IS NOT NULL
          AND derivation_kind NOT IN ('manifest_sync', 'manifest_alert')
    ),
    log_derived AS (
        SELECT * FROM scoped_events WHERE log_index IS NOT NULL
    ),
    boundary_events AS (
        SELECT * FROM scoped_events WHERE log_index IS NULL
    ),
    canonical_raw_log_bounds AS (
        SELECT MIN(raw_logs.block_number)::BIGINT AS min_block_number,
               MAX(raw_logs.block_number)::BIGINT AS max_block_number
        FROM raw_logs
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = 'base-mainnet'
          AND raw_logs.block_number BETWEEN 17571485 AND 46954147
          AND raw_logs.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
          AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
    )
    SELECT
        (SELECT COUNT(*)::BIGINT FROM log_derived) AS log_derived_event_count,
        (
            SELECT COUNT(*)::BIGINT
            FROM log_derived event
            WHERE NOT EXISTS (
                SELECT 1
                FROM raw_logs raw_log
                JOIN chain_lineage lineage
                  ON lineage.chain_id = raw_log.chain_id
                 AND lineage.block_hash = raw_log.block_hash
                WHERE raw_log.chain_id = event.chain_id
                  AND raw_log.block_hash = event.block_hash
                  AND raw_log.log_index = event.log_index
                  AND raw_log.transaction_hash = event.transaction_hash
                  AND raw_log.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
                  AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
            )
        ) AS missing_log_derived_raw_fact_count,
        (SELECT COUNT(*)::BIGINT FROM boundary_events) AS boundary_event_count,
        (
            SELECT COUNT(*)::BIGINT
            FROM boundary_events event
            WHERE NOT EXISTS (
                SELECT 1
                FROM chain_lineage lineage
                WHERE lineage.chain_id = event.chain_id
                  AND lineage.block_hash = event.block_hash
                  AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
            )
        ) AS missing_boundary_lineage_count,
        (SELECT min_block_number FROM canonical_raw_log_bounds) AS canonical_raw_log_min_block,
        (SELECT max_block_number FROM canonical_raw_log_bounds) AS canonical_raw_log_max_block
    "#
}

fn raw_fact_completeness_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<BaseNormalizedRederiveRawFactCompleteness> {
    Ok(BaseNormalizedRederiveRawFactCompleteness {
        log_derived_event_count: row.try_get("log_derived_event_count")?,
        missing_log_derived_raw_fact_count: row.try_get("missing_log_derived_raw_fact_count")?,
        boundary_event_count: row.try_get("boundary_event_count")?,
        missing_boundary_lineage_count: row.try_get("missing_boundary_lineage_count")?,
        canonical_raw_log_min_block: row.try_get("canonical_raw_log_min_block")?,
        canonical_raw_log_max_block: row.try_get("canonical_raw_log_max_block")?,
    })
}
