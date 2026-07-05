use anyhow::{Context, Result};
use sqlx::Row;

use super::{
    BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND, BASE_NORMALIZED_REDERIVE_CHAIN_ID,
    BASE_NORMALIZED_REDERIVE_CURSOR_KIND, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BaseNormalizedRederiveCounts, BaseNormalizedRederiveCursorCensus,
    BaseNormalizedRederiveDerivationKindCensus, BaseNormalizedRederiveRawFactCompleteness,
    checkpoint_adapters, current_projection_replay_status_projections, cursor_kinds,
    reverse_claim_derivation_kind, reverse_claim_source_families, subregistry_derivation_kinds,
    subregistry_source_families, unwrapped_authority_derivation_kind,
    unwrapped_authority_source_families,
};

pub(super) async fn load_counts_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
    replay_target_block: i64,
) -> Result<BaseNormalizedRederiveCounts> {
    let row = sqlx::query(counts_sql())
        .bind(deployment_profile)
        .bind(checkpoint_adapters())
        .bind(cursor_kinds())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .bind(current_projection_replay_status_projections())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive census")?;
    counts_from_row(&row)
}

pub(super) async fn load_derivation_kind_census_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<Vec<BaseNormalizedRederiveDerivationKindCensus>> {
    let rows = sqlx::query(derivation_kind_census_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive derivation-kind census")?;
    derivation_kind_census_rows(rows)
}

pub(super) async fn load_cursor_census_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederiveCursorCensus> {
    let rows = sqlx::query(cursor_census_sql())
        .bind(deployment_profile)
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .bind(cursor_kinds())
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive cursor census")?;
    cursor_census_rows(rows)
}

pub(super) async fn load_raw_fact_completeness_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<BaseNormalizedRederiveRawFactCompleteness> {
    let row = sqlx::query(raw_fact_completeness_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive raw-fact completeness")?;
    raw_fact_completeness_from_row(&row)
}

pub(super) async fn load_max_affected_block_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    canonical_raw_log_head: i64,
) -> Result<Option<i64>> {
    sqlx::query_scalar(max_affected_block_sql())
        .bind(canonical_raw_log_head)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive max affected block")
}

pub(super) async fn load_reset_replay_cursor_target_block_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<Option<i64>> {
    sqlx::query_scalar(reset_replay_cursor_target_block_sql())
        .bind(deployment_profile)
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
        .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive reset replay cursor target")
}

fn counts_sql() -> &'static str {
    r#"
    WITH
    scoped_events AS (
        SELECT normalized_event_id
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND block_number BETWEEN 17571485 AND $4
          AND block_hash IS NOT NULL
          AND (
              (derivation_kind = $5 AND source_family = ANY($6::TEXT[]))
              OR (derivation_kind = ANY($7::TEXT[]) AND source_family = ANY($8::TEXT[]))
              OR (derivation_kind = $9 AND source_family = ANY($10::TEXT[]))
          )
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
        (SELECT COUNT(*)::BIGINT FROM current_projection_replay_status WHERE projection = ANY($11::TEXT[])) AS current_projection_replay_status,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_cursors WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($3::TEXT[])) AS replay_cursor_rows,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoints WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($3::TEXT[]) AND adapter = ANY($2::TEXT[])) AS adapter_checkpoint_rows,
        (SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoint_items WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($3::TEXT[]) AND adapter = ANY($2::TEXT[])) AS adapter_checkpoint_item_rows
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
        current_projection_replay_status: row.try_get("current_projection_replay_status")?,
        replay_cursor_rows: row.try_get("replay_cursor_rows")?,
        adapter_checkpoint_rows: row.try_get("adapter_checkpoint_rows")?,
        adapter_checkpoint_item_rows: row.try_get("adapter_checkpoint_item_rows")?,
    })
}

fn derivation_kind_census_sql() -> &'static str {
    r#"
    SELECT
        derivation_kind,
        source_family,
        COUNT(*)::BIGINT AS row_count,
        MIN(block_number)::BIGINT AS min_block_number,
        MAX(block_number)::BIGINT AS max_block_number,
        (
            (derivation_kind = $2 AND source_family = ANY($3::TEXT[]))
            OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[]))
            OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))
        ) AS rederivable
    FROM normalized_events
    WHERE chain_id = 'base-mainnet'
      AND block_number BETWEEN 17571485 AND $1
      AND block_hash IS NOT NULL
    GROUP BY derivation_kind, source_family, rederivable
    ORDER BY rederivable DESC, derivation_kind, source_family
    "#
}

fn derivation_kind_census_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<BaseNormalizedRederiveDerivationKindCensus>> {
    rows.into_iter()
        .map(|row| {
            Ok(BaseNormalizedRederiveDerivationKindCensus {
                derivation_kind: row.try_get("derivation_kind")?,
                source_family: row.try_get("source_family")?,
                row_count: row.try_get("row_count")?,
                min_block_number: row.try_get("min_block_number")?,
                max_block_number: row.try_get("max_block_number")?,
                rederivable: row.try_get("rederivable")?,
            })
        })
        .collect()
}

fn cursor_census_sql() -> &'static str {
    r#"
    SELECT cursor_kind, COUNT(*)::BIGINT AS row_count
    FROM normalized_replay_cursors
    WHERE deployment_profile = $1
      AND chain_id = $2
      AND cursor_kind = ANY($3::TEXT[])
    GROUP BY cursor_kind
    ORDER BY cursor_kind
    "#
}

fn cursor_census_rows(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<BaseNormalizedRederiveCursorCensus> {
    let mut census = BaseNormalizedRederiveCursorCensus::default();
    for row in rows {
        let cursor_kind: String = row.try_get("cursor_kind")?;
        let row_count: i64 = row.try_get("row_count")?;
        match cursor_kind.as_str() {
            BASE_NORMALIZED_REDERIVE_CURSOR_KIND => {
                census.raw_fact_replay_cursor_rows = row_count;
            }
            BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND => {
                census.post_replay_live_adapter_backlog_cursor_rows = row_count;
            }
            _ => {}
        }
    }
    Ok(census)
}

fn raw_fact_completeness_sql() -> &'static str {
    r#"
    WITH scoped_events AS (
        SELECT chain_id, block_hash, transaction_hash, log_index
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
    log_derived AS (
        SELECT chain_id, block_hash, transaction_hash, log_index
        FROM scoped_events
        WHERE log_index IS NOT NULL
    ),
    boundary_events AS (
        SELECT chain_id, block_hash
        FROM scoped_events
        WHERE log_index IS NULL
    ),
    canonical_raw_log_bounds AS (
        SELECT MIN(raw_logs.block_number)::BIGINT AS min_block_number,
               MAX(raw_logs.block_number)::BIGINT AS max_block_number
        FROM raw_logs
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = 'base-mainnet'
          AND raw_logs.block_number BETWEEN 17571485 AND $1
          AND raw_logs.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
          AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
    ),
    canonical_raw_log_head AS (
        SELECT MAX(raw_logs.block_number)::BIGINT AS head_block
        FROM raw_logs
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = 'base-mainnet'
          AND raw_logs.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
          AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
    )
    SELECT
        $1::BIGINT AS replay_target_block,
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
        (SELECT max_block_number FROM canonical_raw_log_bounds) AS canonical_raw_log_max_block,
        (SELECT head_block FROM canonical_raw_log_head) AS canonical_raw_log_head_block
    "#
}

fn max_affected_block_sql() -> &'static str {
    r#"
    SELECT MAX(block_number)::BIGINT
    FROM normalized_events
    WHERE chain_id = 'base-mainnet'
      AND block_number BETWEEN 17571485 AND $1
      AND block_hash IS NOT NULL
      AND (
          (derivation_kind = $2 AND source_family = ANY($3::TEXT[]))
          OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[]))
          OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))
      )
    "#
}

fn reset_replay_cursor_target_block_sql() -> &'static str {
    r#"
    SELECT MAX(target_block_number)::BIGINT
    FROM normalized_replay_cursors
    WHERE deployment_profile = $1
      AND chain_id = $2
      AND cursor_kind = $3
      AND range_start_block_number = $4
      AND COALESCE(last_completed_block_number, range_start_block_number - 1) < target_block_number
    "#
}

fn raw_fact_completeness_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<BaseNormalizedRederiveRawFactCompleteness> {
    Ok(BaseNormalizedRederiveRawFactCompleteness {
        replay_target_block: row.try_get("replay_target_block")?,
        log_derived_event_count: row.try_get("log_derived_event_count")?,
        missing_log_derived_raw_fact_count: row.try_get("missing_log_derived_raw_fact_count")?,
        boundary_event_count: row.try_get("boundary_event_count")?,
        missing_boundary_lineage_count: row.try_get("missing_boundary_lineage_count")?,
        canonical_raw_log_min_block: row.try_get("canonical_raw_log_min_block")?,
        canonical_raw_log_max_block: row.try_get("canonical_raw_log_max_block")?,
        canonical_raw_log_head_block: row.try_get("canonical_raw_log_head_block")?,
    })
}
