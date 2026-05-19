use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use tracing::info;

use super::{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayCursor};

const CURRENT_PROJECTION_TABLES: &[&str] = &[
    "address_names_current",
    "children_current",
    "name_current",
    "permissions_current",
    "primary_names_current",
    "record_inventory_current",
    "resolver_current",
];

const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
    "normalized_events_namespace_idx",
    "normalized_events_kind_idx",
    "normalized_events_manifest_idx",
    "normalized_events_chain_position_idx",
    "normalized_events_name_projection_replay_idx",
    "normalized_events_resource_projection_replay_idx",
    "normalized_events_name_relevant_projection_idx",
    "normalized_events_record_inventory_resource_replay_idx",
];

const TEMPORARY_REPLAY_INDEXES: &[&str] = &[
    "normalized_events_replay_latest_resolver_tmp_idx",
    "normalized_events_replay_latest_record_version_tmp_idx",
    "normalized_events_replay_latest_registrar_tmp_idx",
];

pub(super) async fn prepare_deferred_projection_indexes_for_fresh_replay(
    pool: &PgPool,
    cursor: &NormalizedReplayCursor,
) -> Result<()> {
    if cursor.next_block_number > cursor.target_block_number {
        return Ok(());
    }

    let already_deferred = any_index_missing(pool, DEFERRED_NORMALIZED_EVENT_INDEXES).await?
        || any_index_exists(pool, TEMPORARY_REPLAY_INDEXES).await?;
    let projection_tables_empty = current_projection_tables_empty(pool).await?;
    if !should_defer_projection_indexes(cursor, already_deferred, projection_tables_empty) {
        return Ok(());
    }

    ensure_temporary_replay_indexes(pool).await?;
    drop_deferred_projection_indexes(pool).await?;

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        range_start_block_number = cursor.range_start_block_number,
        next_block_number = cursor.next_block_number,
        target_block_number = cursor.target_block_number,
        projection_tables_empty,
        already_deferred,
        "normalized replay projection indexes deferred for fresh catch-up"
    );
    Ok(())
}

pub(super) async fn ensure_projection_indexes_after_catchup(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<()> {
    if chains.is_empty()
        || !all_configured_cursors_complete(pool, deployment_profile, chains).await?
    {
        return Ok(());
    }
    if !projection_indexes_need_restore(pool).await? {
        return Ok(());
    }

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "normalized replay complete; rebuilding deferred projection indexes"
    );

    ensure_deferred_projection_indexes_ready(pool, deployment_profile, chains).await?;

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "deferred normalized replay projection indexes are ready"
    );
    Ok(())
}

pub(super) async fn restore_deferred_projection_indexes(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<()> {
    if !projection_indexes_need_restore(pool).await? {
        return Ok(());
    }

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "normalized replay selected closure/dependency adapters; restoring deferred projection indexes before failing closed"
    );

    ensure_deferred_projection_indexes_ready(pool, deployment_profile, chains).await?;

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile,
        chain_count = chains.len(),
        "deferred normalized replay projection indexes are ready"
    );
    Ok(())
}

async fn ensure_deferred_projection_indexes_ready(
    pool: &PgPool,
    _deployment_profile: &str,
    _chains: &[String],
) -> Result<()> {
    if !projection_indexes_need_restore(pool).await? {
        return Ok(());
    }

    create_deferred_projection_indexes(pool).await?;
    drop_temporary_replay_indexes(pool).await
}

async fn projection_indexes_need_restore(pool: &PgPool) -> Result<bool> {
    Ok(
        any_index_missing(pool, DEFERRED_NORMALIZED_EVENT_INDEXES).await?
            || any_index_exists(pool, TEMPORARY_REPLAY_INDEXES).await?,
    )
}

fn should_defer_projection_indexes(
    cursor: &NormalizedReplayCursor,
    already_deferred: bool,
    projection_tables_empty: bool,
) -> bool {
    already_deferred || (projection_tables_empty && cursor.last_replayed_at.is_none())
}

async fn current_projection_tables_empty(pool: &PgPool) -> Result<bool> {
    for table in CURRENT_PROJECTION_TABLES {
        if !relation_exists(pool, table).await? {
            continue;
        }
        let has_rows = sqlx::query_scalar::<_, bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1)"
        ))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to inspect current projection table {table}"))?;
        if has_rows {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    fn cursor(last_replayed_at: Option<OffsetDateTime>) -> NormalizedReplayCursor {
        NormalizedReplayCursor {
            range_start_block_number: 1,
            next_block_number: 10,
            target_block_number: 20,
            last_replayed_at,
        }
    }

    #[test]
    fn defers_indexes_when_already_deferred() {
        assert!(should_defer_projection_indexes(
            &cursor(Some(OffsetDateTime::UNIX_EPOCH)),
            true,
            false
        ));
    }

    #[test]
    fn defers_indexes_for_initial_empty_projection_replay() {
        assert!(should_defer_projection_indexes(&cursor(None), false, true));
    }

    #[test]
    fn keeps_projection_indexes_after_initial_replay_completed() {
        assert!(!should_defer_projection_indexes(
            &cursor(Some(OffsetDateTime::UNIX_EPOCH)),
            false,
            true
        ));
    }
}

pub(super) async fn all_configured_cursors_complete(
    pool: &PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<bool> {
    let cursor_rows = sqlx::query(
        r#"
        WITH configured_chains AS (
            SELECT DISTINCT UNNEST($3::TEXT[]) AS chain_id
        )
        SELECT
            configured_chains.chain_id,
            cursor.next_block_number,
            cursor.target_block_number
        FROM configured_chains
        LEFT JOIN normalized_replay_cursors AS cursor
          ON cursor.deployment_profile = $1
         AND cursor.cursor_kind = $2
         AND cursor.chain_id = configured_chains.chain_id
        "#,
    )
    .bind(deployment_profile)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(chains)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to inspect normalized replay cursor completion for {deployment_profile}")
    })?;

    let mut missing_cursor_chains = Vec::new();
    for row in cursor_rows {
        let chain_id = row
            .try_get::<String, _>("chain_id")
            .context("missing configured chain id")?;
        let next_block_number = row
            .try_get::<Option<i64>, _>("next_block_number")
            .context("missing normalized replay next block number")?;
        let target_block_number = row
            .try_get::<Option<i64>, _>("target_block_number")
            .context("missing normalized replay target block number")?;
        match (next_block_number, target_block_number) {
            (Some(next), Some(target)) if next > target => {}
            (Some(_), Some(_)) => return Ok(false),
            _ => missing_cursor_chains.push(chain_id),
        }
    }

    for chain in missing_cursor_chains {
        if chain_has_canonical_raw_logs(pool, &chain).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn chain_has_canonical_raw_logs(pool: &PgPool, chain: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT (
            SELECT raw_log_id
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY raw_logs.block_number DESC
            LIMIT 1
        ) IS NOT NULL
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to inspect canonical raw logs for {chain}"))
}

async fn relation_exists(pool: &PgPool, relation: &str) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to inspect relation {relation}"))?;
    Ok(exists)
}

async fn any_index_missing(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    for index in indexes {
        if !relation_exists(pool, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn any_index_exists(pool: &PgPool, indexes: &[&str]) -> Result<bool> {
    for index in indexes {
        if relation_exists(pool, index).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn ensure_temporary_replay_indexes(pool: &PgPool) -> Result<()> {
    execute_ddl(
        pool,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_resolver_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind = 'ResolverChanged'
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_record_version_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind = 'RecordVersionChanged'
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX IF NOT EXISTS normalized_events_replay_latest_registrar_tmp_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             log_index DESC NULLS LAST,
             normalized_event_id DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND event_kind IN (
               'RegistrationGranted',
               'RegistrationRenewed',
               'ExpiryChanged',
               'TokenControlTransferred'
           )
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await
}

async fn drop_deferred_projection_indexes(pool: &PgPool) -> Result<()> {
    for index in DEFERRED_NORMALIZED_EVENT_INDEXES {
        execute_ddl(pool, &format!("DROP INDEX IF EXISTS {index}")).await?;
    }
    Ok(())
}

async fn create_deferred_projection_indexes(pool: &PgPool) -> Result<()> {
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_namespace_idx
         ON normalized_events (namespace, normalized_event_id DESC)",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_kind_idx
         ON normalized_events (event_kind, normalized_event_id DESC)",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_manifest_idx
         ON normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
         WHERE source_manifest_id IS NOT NULL",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_chain_position_idx
         ON normalized_events (chain_id, block_number DESC, normalized_event_id DESC)
         WHERE block_number IS NOT NULL",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_projection_replay_idx
         ON normalized_events (
             logical_name_id,
             block_number DESC NULLS LAST,
             chain_id ASC NULLS LAST,
             block_hash DESC NULLS LAST,
             transaction_hash DESC NULLS LAST,
             log_index DESC NULLS LAST,
             event_identity DESC
         )
         WHERE logical_name_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resource_projection_replay_idx
         ON normalized_events (
             resource_id,
             block_number DESC NULLS LAST,
             chain_id ASC NULLS LAST,
             block_hash DESC NULLS LAST,
             transaction_hash DESC NULLS LAST,
             log_index DESC NULLS LAST,
             event_identity DESC
         )
         WHERE resource_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_relevant_projection_idx
         ON normalized_events (
             logical_name_id,
             block_number ASC NULLS FIRST,
             log_index ASC NULLS LAST,
             event_identity ASC
         )
         WHERE logical_name_id IS NOT NULL
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await?;
    execute_ddl(
        pool,
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_record_inventory_resource_replay_idx
         ON normalized_events (
             resource_id,
             block_number ASC,
             log_index ASC NULLS FIRST,
             normalized_event_id ASC
         )
         WHERE resource_id IS NOT NULL
           AND logical_name_id IS NOT NULL
           AND chain_id IS NOT NULL
           AND block_number IS NOT NULL
           AND block_hash IS NOT NULL
           AND derivation_kind IN (
               'ens_v1_unwrapped_authority',
               'ens_v2_resolver'
           )
           AND event_kind IN (
               'RecordChanged',
               'RecordVersionChanged',
               'ResolverChanged'
           )
           AND canonicality_state IN (
               'canonical'::canonicality_state,
               'safe'::canonicality_state,
               'finalized'::canonicality_state
           )",
    )
    .await
}

async fn drop_temporary_replay_indexes(pool: &PgPool) -> Result<()> {
    for index in TEMPORARY_REPLAY_INDEXES {
        execute_ddl(pool, &format!("DROP INDEX IF EXISTS {index}")).await?;
    }
    Ok(())
}

async fn execute_ddl(pool: &PgPool, statement: &str) -> Result<()> {
    sqlx::query(statement)
        .execute(pool)
        .await
        .with_context(|| format!("failed to execute normalized replay index DDL: {statement}"))?;
    Ok(())
}
