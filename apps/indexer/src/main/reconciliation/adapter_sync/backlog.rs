use anyhow::{Context, Result};
use tracing::info;

use crate::normalized_replay_catchup::CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS;

use super::sync_live_adapter_state_from_persisted_raw_payloads;

const CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG: &str = "post_replay_live_adapter_backlog";
const DEFAULT_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE: i64 = 1;
const MAX_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE: i64 = 64;
const LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE_ENV: &str =
    "BIGNAME_INDEXER_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiveAdapterBacklogSyncSummary {
    pub(crate) chain_count: usize,
    pub(crate) selected_block_count: usize,
    pub(crate) scanned_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) normalized_event_synced_count: usize,
    pub(crate) normalized_event_inserted_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BacklogCursor {
    next_block_number: i64,
    target_block_number: i64,
}

pub(crate) async fn sync_live_adapter_backlog_after_normalized_replay(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<LiveAdapterBacklogSyncSummary> {
    let mut aggregate = LiveAdapterBacklogSyncSummary::default();

    for chain in chains {
        let mut cursor = match ensure_backlog_cursor(pool, deployment_profile, chain).await? {
            Some(cursor) => cursor,
            None => continue,
        };
        if cursor.next_block_number > cursor.target_block_number {
            continue;
        }

        aggregate.chain_count += 1;
        loop {
            let block_hashes = load_backlog_block_hash_page(
                pool,
                chain,
                cursor.next_block_number,
                cursor.target_block_number,
            )
            .await?;
            if block_hashes.is_empty() {
                advance_backlog_cursor_to_target(pool, deployment_profile, chain, cursor).await?;
                break;
            }

            let completed_to_block = block_hashes
                .last()
                .map(|block| block.block_number)
                .context("backlog block page unexpectedly empty")?;
            let hashes = block_hashes
                .into_iter()
                .map(|block| block.block_hash)
                .collect::<Vec<_>>();
            info!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                chain,
                from_block = cursor.next_block_number,
                to_block = completed_to_block,
                target_block_number = cursor.target_block_number,
                block_hash_count = hashes.len(),
                "post-replay live raw payload adapter backlog page selected"
            );
            let summary = sync_live_adapter_state_from_persisted_raw_payloads(pool, chain, &hashes)
                .await
                .with_context(|| {
                    format!(
                        "failed to sync post-replay live adapter backlog for {deployment_profile}/{chain} through block {completed_to_block}"
                    )
                })?;

            aggregate.selected_block_count += hashes.len();
            aggregate.scanned_log_count += summary.scanned_log_count;
            aggregate.matched_log_count += summary.matched_log_count;
            aggregate.normalized_event_synced_count += summary.total_synced_count;
            aggregate.normalized_event_inserted_count += summary.total_inserted_count;

            cursor = advance_backlog_cursor(
                pool,
                deployment_profile,
                chain,
                completed_to_block,
                hashes.len(),
                &summary,
            )
            .await?;
            if cursor.next_block_number > cursor.target_block_number {
                break;
            }
        }
    }

    if aggregate.selected_block_count > 0 {
        info!(
            service = "indexer",
            command = "poll",
            deployment_profile,
            chain_count = aggregate.chain_count,
            selected_block_count = aggregate.selected_block_count,
            scanned_log_count = aggregate.scanned_log_count,
            matched_log_count = aggregate.matched_log_count,
            normalized_event_synced_count = aggregate.normalized_event_synced_count,
            normalized_event_inserted_count = aggregate.normalized_event_inserted_count,
            "post-replay live raw payload adapter backlog synced"
        );
    }

    Ok(aggregate)
}

async fn ensure_backlog_cursor(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<BacklogCursor>> {
    if let Some(cursor) = load_existing_backlog_cursor(pool, deployment_profile, chain).await? {
        return Ok(Some(
            refresh_backlog_cursor_target(pool, deployment_profile, chain, cursor).await?,
        ));
    }

    let Some(replay_target_block_number) =
        load_completed_replay_target_block(pool, deployment_profile, chain).await?
    else {
        return Ok(None);
    };
    let range_start_block_number = replay_target_block_number
        .checked_add(1)
        .context("post-replay live adapter backlog start block overflowed")?;
    let Some(target_block_number) =
        load_backlog_target_block(pool, chain, range_start_block_number).await?
    else {
        return Ok(None);
    };

    let cursor = sqlx::query_as::<_, (i64, i64)>(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ($1, $2, $3, $4, $4, $5)
        ON CONFLICT (deployment_profile, chain_id, cursor_kind) DO UPDATE
        SET updated_at = now()
        RETURNING next_block_number, target_block_number
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(range_start_block_number)
    .bind(target_block_number)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to initialize post-replay live adapter backlog cursor for {deployment_profile}/{chain}"
        )
    })?;

    Ok(Some(BacklogCursor {
        next_block_number: cursor.0,
        target_block_number: cursor.1,
    }))
}

async fn refresh_backlog_cursor_target(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
) -> Result<BacklogCursor> {
    let Some(target_block_number) =
        load_backlog_target_block(pool, chain, cursor.next_block_number).await?
    else {
        return Ok(cursor);
    };
    if target_block_number <= cursor.target_block_number {
        return Ok(cursor);
    }

    let row = sqlx::query_as::<_, (i64, i64)>(
        r#"
        UPDATE normalized_replay_cursors
        SET
            target_block_number = GREATEST(target_block_number, $4),
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        RETURNING next_block_number, target_block_number
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(target_block_number)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to refresh post-replay live adapter backlog cursor target for {deployment_profile}/{chain}"
        )
    })?;

    Ok(BacklogCursor {
        next_block_number: row.0,
        target_block_number: row.1,
    })
}

async fn load_existing_backlog_cursor(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<BacklogCursor>> {
    let cursor = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT next_block_number, target_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load post-replay live adapter backlog cursor for {deployment_profile}/{chain}"
        )
    })?;

    Ok(cursor.map(|cursor| BacklogCursor {
        next_block_number: cursor.0,
        target_block_number: cursor.1,
    }))
}

async fn load_completed_replay_target_block(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT target_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND next_block_number > target_block_number
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load completed normalized replay target for {deployment_profile}/{chain}"
        )
    })
}

async fn load_backlog_target_block(
    pool: &sqlx::PgPool,
    chain: &str,
    range_start_block_number: i64,
) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(raw_logs.block_number)
        FROM raw_logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1
          AND raw_logs.block_number >= $2
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND raw_logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(range_start_block_number)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load post-replay live adapter backlog target for {chain}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BacklogBlock {
    block_number: i64,
    block_hash: String,
}

async fn load_backlog_block_hash_page(
    pool: &sqlx::PgPool,
    chain: &str,
    next_block_number: i64,
    target_block_number: i64,
) -> Result<Vec<BacklogBlock>> {
    let block_batch_size = live_adapter_backlog_block_batch_size();
    let rows = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT DISTINCT lineage.block_number, lineage.block_hash
        FROM raw_logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1
          AND raw_logs.block_number >= $2
          AND raw_logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND raw_logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY lineage.block_number, lineage.block_hash
        LIMIT $4
        "#,
    )
    .bind(chain)
    .bind(next_block_number)
    .bind(target_block_number)
    .bind(block_batch_size)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load post-replay live adapter backlog block page for {chain} from {next_block_number} to {target_block_number}"
        )
    })?;

    Ok(rows
        .into_iter()
        .map(|row| BacklogBlock {
            block_number: row.0,
            block_hash: row.1,
        })
        .collect())
}

fn live_adapter_backlog_block_batch_size() -> i64 {
    std::env::var(LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE_ENV)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE))
        .unwrap_or(DEFAULT_LIVE_ADAPTER_BACKLOG_BLOCK_BATCH_SIZE)
}

async fn advance_backlog_cursor(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    completed_to_block: i64,
    selected_block_count: usize,
    summary: &crate::reconciliation::PersistedRawPayloadAdapterSyncSummary,
) -> Result<BacklogCursor> {
    let next_block_number = completed_to_block
        .checked_add(1)
        .context("post-replay live adapter backlog next block overflowed")?;
    let selected_block_count = i64::try_from(selected_block_count)
        .context("post-replay live adapter backlog block count does not fit in i64")?;
    let scanned_log_count = i64::try_from(summary.scanned_log_count)
        .context("post-replay live adapter backlog scanned log count does not fit in i64")?;
    let matched_log_count = i64::try_from(summary.matched_log_count)
        .context("post-replay live adapter backlog matched log count does not fit in i64")?;
    let normalized_event_synced_count = i64::try_from(summary.total_synced_count)
        .context("post-replay live adapter backlog synced count does not fit in i64")?;
    let normalized_event_inserted_count = i64::try_from(summary.total_inserted_count)
        .context("post-replay live adapter backlog inserted count does not fit in i64")?;

    let row = sqlx::query_as::<_, (i64, i64)>(
        r#"
        UPDATE normalized_replay_cursors
        SET
            next_block_number = GREATEST(next_block_number, $4),
            last_completed_block_number = CASE
                WHEN last_completed_block_number IS NULL THEN $5
                ELSE GREATEST(last_completed_block_number, $5)
            END,
            last_selected_block_count = $6,
            last_scanned_raw_log_count = $7,
            last_matched_raw_log_count = $8,
            last_normalized_event_synced_count = $9,
            last_normalized_event_inserted_count = $10,
            last_replayed_at = now(),
            last_failure_reason = NULL,
            last_failure_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        RETURNING next_block_number, target_block_number
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(next_block_number)
    .bind(completed_to_block)
    .bind(selected_block_count)
    .bind(scanned_log_count)
    .bind(matched_log_count)
    .bind(normalized_event_synced_count)
    .bind(normalized_event_inserted_count)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to advance post-replay live adapter backlog cursor for {deployment_profile}/{chain} through block {completed_to_block}"
        )
    })?;

    Ok(BacklogCursor {
        next_block_number: row.0,
        target_block_number: row.1,
    })
}

async fn advance_backlog_cursor_to_target(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: BacklogCursor,
) -> Result<()> {
    let next_block_number = cursor
        .target_block_number
        .checked_add(1)
        .context("post-replay live adapter backlog target completion overflowed")?;
    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET
            next_block_number = GREATEST(next_block_number, $4),
            last_completed_block_number = CASE
                WHEN last_completed_block_number IS NULL THEN $5
                ELSE GREATEST(last_completed_block_number, $5)
            END,
            last_replayed_at = now(),
            last_failure_reason = NULL,
            last_failure_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_POST_REPLAY_LIVE_ADAPTER_BACKLOG)
    .bind(next_block_number)
    .bind(cursor.target_block_number)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to complete empty post-replay live adapter backlog cursor for {deployment_profile}/{chain}"
        )
    })?;

    Ok(())
}
