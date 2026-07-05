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
    Ok(BaseNormalizedRederiveCounts {
        normalized_events: load_scoped_event_count_from(transaction, replay_target_block).await?,
        resources: load_count_from(transaction, resource_count_sql(), "resources").await?,
        token_lineages: load_count_from(transaction, token_lineage_count_sql(), "token_lineages")
            .await?,
        name_surfaces: load_count_from(transaction, name_surface_count_sql(), "name_surfaces")
            .await?,
        surface_bindings: load_count_from(
            transaction,
            surface_binding_count_sql(),
            "surface_bindings",
        )
        .await?,
        name_current: load_count_from(transaction, name_current_count_sql(), "name_current")
            .await?,
        address_names_current: load_count_from(
            transaction,
            address_names_current_count_sql(),
            "address_names_current",
        )
        .await?,
        children_current: load_count_from(
            transaction,
            children_current_count_sql(),
            "children_current",
        )
        .await?,
        permissions_current: load_count_from(
            transaction,
            permissions_current_count_sql(),
            "permissions_current",
        )
        .await?,
        record_inventory_current: load_count_from(
            transaction,
            record_inventory_current_count_sql(),
            "record_inventory_current",
        )
        .await?,
        projection_normalized_event_changes: load_projection_change_count_from(
            transaction,
            replay_target_block,
        )
        .await?,
        current_projection_replay_status: load_current_projection_replay_status_count_from(
            transaction,
        )
        .await?,
        replay_cursor_rows: load_replay_cursor_count_from(transaction, deployment_profile).await?,
        adapter_checkpoint_rows: load_adapter_checkpoint_count_from(
            transaction,
            deployment_profile,
        )
        .await?,
        adapter_checkpoint_item_rows: load_adapter_checkpoint_item_count_from(
            transaction,
            deployment_profile,
        )
        .await?,
    })
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

async fn load_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: impl AsRef<str>,
    count_name: &str,
) -> Result<i64> {
    sqlx::query_scalar(sql.as_ref())
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to load Base normalized-event rederive {count_name} count")
        })
}

async fn load_scoped_event_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<i64> {
    sqlx::query_scalar(scoped_event_count_sql())
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive normalized_events count")
}

async fn load_projection_change_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<i64> {
    let sql = projection_change_count_sql();
    sqlx::query_scalar(&sql)
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .context(
            "failed to load Base normalized-event rederive projection_normalized_event_changes count",
        )
}

async fn load_current_projection_replay_status_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64> {
    sqlx::query_scalar(current_projection_replay_status_count_sql())
        .bind(current_projection_replay_status_projections())
        .fetch_one(&mut **transaction)
        .await
        .context(
            "failed to load Base normalized-event rederive current_projection_replay_status count",
        )
}

async fn load_replay_cursor_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    sqlx::query_scalar(replay_cursor_count_sql())
        .bind(deployment_profile)
        .bind(cursor_kinds())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive replay_cursor_rows count")
}

async fn load_adapter_checkpoint_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    sqlx::query_scalar(adapter_checkpoint_count_sql())
        .bind(deployment_profile)
        .bind(cursor_kinds())
        .bind(checkpoint_adapters())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive adapter_checkpoint_rows count")
}

async fn load_adapter_checkpoint_item_count_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    sqlx::query_scalar(adapter_checkpoint_item_count_sql())
        .bind(deployment_profile)
        .bind(cursor_kinds())
        .bind(checkpoint_adapters())
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive adapter_checkpoint_item_rows count")
}

pub(super) fn scoped_event_count_sql() -> &'static str {
    r#"SELECT COALESCE(SUM(row_count), 0)::BIGINT FROM (
        SELECT COUNT(*)::BIGINT AS row_count FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = $2 AND source_family = ANY($3::TEXT[])
        UNION ALL
        SELECT COUNT(*)::BIGINT AS row_count FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[])
        UNION ALL
        SELECT COUNT(*)::BIGINT AS row_count FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = $6 AND source_family = ANY($7::TEXT[])
    ) scoped_pair_counts"#
}

fn scoped_events_sql() -> &'static str {
    r#"SELECT normalized_event_id FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = $2 AND source_family = ANY($3::TEXT[])
    UNION ALL SELECT normalized_event_id FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[])
    UNION ALL SELECT normalized_event_id FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND derivation_kind = $6 AND source_family = ANY($7::TEXT[])"#
}

pub(super) fn projection_change_count_sql() -> String {
    format!(
        "WITH scoped_events AS ({}) SELECT COUNT(*)::BIGINT FROM scoped_events event JOIN projection_normalized_event_changes change ON change.normalized_event_id = event.normalized_event_id",
        scoped_events_sql()
    )
}

fn scoped_identity_predicate_for(alias: &str) -> String {
    format!(
        "{alias}.chain_id = 'base-mainnet' AND {alias}.provenance->>'adapter' = 'ens_v1_unwrapped_authority'"
    )
}

fn resource_count_sql() -> String {
    scoped_identity_count_sql("resources")
}

fn token_lineage_count_sql() -> String {
    scoped_identity_count_sql("token_lineages")
}

fn name_surface_count_sql() -> String {
    scoped_identity_count_sql("name_surfaces")
}

fn surface_binding_count_sql() -> String {
    scoped_identity_count_sql("surface_bindings")
}

fn scoped_identity_count_sql(table: &str) -> String {
    format!(
        "SELECT COUNT(*)::BIGINT FROM {table} WHERE chain_id = 'base-mainnet' AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'"
    )
}

pub(super) fn name_current_count_sql() -> String {
    identity_projection_count_sql(
        "name_current",
        &["projection.logical_name_id"],
        &[
            ("resources", RESOURCE_IDENTITY_JOIN),
            ("token_lineages", TOKEN_LINEAGE_IDENTITY_JOIN),
            ("name_surfaces", LOGICAL_NAME_IDENTITY_JOIN),
            ("surface_bindings", SURFACE_BINDING_IDENTITY_JOIN),
        ],
    )
}

pub(super) fn address_names_current_count_sql() -> String {
    identity_projection_count_sql(
        "address_names_current",
        &[
            "projection.address",
            "projection.logical_name_id",
            "projection.relation",
        ],
        &[
            ("resources", RESOURCE_IDENTITY_JOIN),
            ("token_lineages", TOKEN_LINEAGE_IDENTITY_JOIN),
            ("name_surfaces", LOGICAL_NAME_IDENTITY_JOIN),
            ("surface_bindings", SURFACE_BINDING_IDENTITY_JOIN),
        ],
    )
}

pub(super) fn children_current_count_sql() -> String {
    identity_projection_count_sql(
        "children_current",
        &[
            "projection.parent_logical_name_id",
            "projection.child_logical_name_id",
            "projection.surface_class",
        ],
        &[
            ("name_surfaces", PARENT_LOGICAL_NAME_IDENTITY_JOIN),
            ("name_surfaces", CHILD_LOGICAL_NAME_IDENTITY_JOIN),
        ],
    )
}

const RESOURCE_IDENTITY_JOIN: &str = "projection.resource_id = identity_scope.resource_id";
const TOKEN_LINEAGE_IDENTITY_JOIN: &str =
    "projection.token_lineage_id = identity_scope.token_lineage_id";
const LOGICAL_NAME_IDENTITY_JOIN: &str =
    "projection.logical_name_id = identity_scope.logical_name_id";
const SURFACE_BINDING_IDENTITY_JOIN: &str =
    "projection.surface_binding_id = identity_scope.surface_binding_id";
const PARENT_LOGICAL_NAME_IDENTITY_JOIN: &str =
    "projection.parent_logical_name_id = identity_scope.logical_name_id";
const CHILD_LOGICAL_NAME_IDENTITY_JOIN: &str =
    "projection.child_logical_name_id = identity_scope.logical_name_id";

fn identity_projection_count_sql(
    projection_table: &str,
    key_columns: &[&str],
    branches: &[(&str, &str)],
) -> String {
    let key_select = key_columns.join(", ");
    let scope_predicate = scoped_identity_predicate_for("identity_scope");
    let union_branches = branches
        .iter()
        .map(|(scope_table, join_predicate)| {
            format!(
                "SELECT {key_select} FROM {scope_table} identity_scope JOIN {projection_table} projection ON {join_predicate} WHERE {scope_predicate}"
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION\n");
    format!(
        "WITH scoped_projection_keys AS ({union_branches}) SELECT COUNT(*)::BIGINT FROM scoped_projection_keys"
    )
}

pub(super) fn permissions_current_count_sql() -> String {
    resource_projection_count_sql("permissions_current")
}

pub(super) fn record_inventory_current_count_sql() -> String {
    resource_projection_count_sql("record_inventory_current")
}

fn resource_projection_count_sql(projection_table: &str) -> String {
    format!(
        "SELECT COUNT(*)::BIGINT FROM resources identity_scope JOIN {projection_table} projection ON projection.resource_id = identity_scope.resource_id WHERE {}",
        scoped_identity_predicate_for("identity_scope")
    )
}

fn current_projection_replay_status_count_sql() -> &'static str {
    "SELECT COUNT(*)::BIGINT FROM current_projection_replay_status WHERE projection = ANY($1::TEXT[])"
}

fn replay_cursor_count_sql() -> &'static str {
    "SELECT COUNT(*)::BIGINT FROM normalized_replay_cursors WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($2::TEXT[])"
}

fn adapter_checkpoint_count_sql() -> &'static str {
    "SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoints WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($2::TEXT[]) AND adapter = ANY($3::TEXT[])"
}

fn adapter_checkpoint_item_count_sql() -> &'static str {
    "SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoint_items WHERE deployment_profile = $1 AND chain_id = 'base-mainnet' AND cursor_kind = ANY($2::TEXT[]) AND adapter = ANY($3::TEXT[])"
}

fn derivation_kind_census_sql() -> &'static str {
    r#"SELECT derivation_kind, source_family, COUNT(*)::BIGINT AS row_count, MIN(block_number)::BIGINT AS min_block_number, MAX(block_number)::BIGINT AS max_block_number, ((derivation_kind = $2 AND source_family = ANY($3::TEXT[])) OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[])) OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))) AS rederivable FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL GROUP BY derivation_kind, source_family, rederivable ORDER BY rederivable DESC, derivation_kind, source_family"#
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
    "SELECT cursor_kind, COUNT(*)::BIGINT AS row_count FROM normalized_replay_cursors WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = ANY($3::TEXT[]) GROUP BY cursor_kind ORDER BY cursor_kind"
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
    r#"SELECT MAX(block_number)::BIGINT FROM normalized_events WHERE chain_id = 'base-mainnet' AND block_number BETWEEN 17571485 AND $1 AND block_hash IS NOT NULL AND ((derivation_kind = $2 AND source_family = ANY($3::TEXT[])) OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[])) OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[])))"#
}

fn reset_replay_cursor_target_block_sql() -> &'static str {
    "SELECT MAX(target_block_number)::BIGINT FROM normalized_replay_cursors WHERE deployment_profile = $1 AND chain_id = $2 AND cursor_kind = $3 AND range_start_block_number = $4 AND COALESCE(last_completed_block_number, range_start_block_number - 1) < target_block_number"
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
