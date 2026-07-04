use anyhow::{Context, Result, bail};
use bigname_storage::sql_row;
use serde_json::json;
use sqlx::{Executor, Postgres, Transaction};
use tracing::info;

use crate::cli::CompactLogStagingArgs;

const RAW_FACT_NORMALIZED_REPLAY_CURSOR: &str = "raw_fact_normalized_events";
const LOG_STAGING_TABLES: [&str; 3] = ["raw_logs", "raw_transactions", "raw_receipts"];

#[derive(Clone, Debug, Eq, PartialEq)]
struct NormalizedReplayReadiness {
    cursor_count: i64,
    remaining_block_count: i64,
    completed_through_block: Option<i64>,
    caught_up: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawFactTableSummary {
    table_name: String,
    estimated_row_count: i64,
    total_bytes: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawFactUncoveredRows {
    table_name: String,
    row_count: i64,
    max_block_number: Option<i64>,
}

pub(crate) async fn compact_log_staging(args: CompactLogStagingArgs) -> Result<()> {
    let (pool, _rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &args.database,
            "bigname-worker",
        )
        .await?;
    let mut tx = pool
        .begin()
        .await
        .context("failed to begin raw-log staging compaction transaction")?;
    lock_log_staging_tables(&mut tx).await?;

    let readiness = load_normalized_replay_readiness(&mut tx).await?;
    let before = load_log_staging_table_summaries(&mut tx).await?;

    if !readiness.caught_up {
        bail!(
            "refusing to compact raw-log staging while normalized replay has {} remaining blocks across {} cursor(s)",
            readiness.remaining_block_count,
            readiness.cursor_count
        );
    }
    if readiness.cursor_count == 0 {
        bail!("refusing to compact raw-log staging without a normalized replay cursor");
    }
    let Some(completed_through_block) = readiness.completed_through_block else {
        bail!("refusing to compact raw-log staging before normalized replay has completed a block");
    };
    let uncovered = load_uncovered_log_staging_rows(&mut tx, completed_through_block).await?;
    if !uncovered.is_empty() {
        bail!(
            "refusing to compact raw-log staging because {} table(s) still contain rows above normalized replay block {}: {}",
            uncovered.len(),
            completed_through_block,
            uncovered_rows_summary(&uncovered)
        );
    }

    if !args.dry_run {
        truncate_log_staging_tables(&mut tx).await?;
    }

    let after = if args.dry_run {
        before.clone()
    } else {
        load_log_staging_table_summaries(&mut tx).await?
    };

    if args.json {
        println!(
            "{}",
            json!({
                "command": "raw-facts compact-log-staging",
                "dry_run": args.dry_run,
                "compacted": !args.dry_run,
                "normalized_replay": {
                    "cursor_count": readiness.cursor_count,
                    "remaining_block_count": readiness.remaining_block_count,
                    "completed_through_block": readiness.completed_through_block,
                    "caught_up": readiness.caught_up,
                },
                "uncovered_rows": uncovered_rows_json(&uncovered),
                "before": raw_fact_table_summaries_json(&before),
                "after": raw_fact_table_summaries_json(&after),
                "estimated_reclaimed_bytes": total_bytes(&before).saturating_sub(total_bytes(&after)),
            })
        );
        tx.commit()
            .await
            .context("failed to commit raw-log staging compaction transaction")?;
        return Ok(());
    }

    info!(
        service = "worker",
        command = "raw_facts compact_log_staging",
        dry_run = args.dry_run,
        compacted = !args.dry_run,
        normalized_replay_cursor_count = readiness.cursor_count,
        normalized_replay_remaining_block_count = readiness.remaining_block_count,
        normalized_replay_completed_through_block = readiness.completed_through_block,
        estimated_reclaimed_bytes = total_bytes(&before).saturating_sub(total_bytes(&after)),
        "raw-log staging compaction completed"
    );

    tx.commit()
        .await
        .context("failed to commit raw-log staging compaction transaction")?;

    Ok(())
}

async fn lock_log_staging_tables(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    tx.execute("LOCK TABLE raw_logs, raw_transactions, raw_receipts IN ACCESS EXCLUSIVE MODE")
        .await
        .context("failed to lock raw-log staging tables for compaction")?;

    Ok(())
}

async fn load_normalized_replay_readiness(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<NormalizedReplayReadiness> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS cursor_count,
            COALESCE(
                SUM(GREATEST(target_block_number - next_block_number + 1, 0)),
                0
            )::BIGINT AS remaining_block_count,
            MIN(last_completed_block_number)::BIGINT AS completed_through_block,
            COALESCE(
                BOOL_AND(
                    next_block_number > target_block_number
                    AND last_failure_reason IS NULL
                    AND last_failure_at IS NULL
                    AND last_completed_block_number IS NOT NULL
                ),
                FALSE
            ) AS caught_up
        FROM normalized_replay_cursors
        WHERE cursor_kind = $1
        "#,
    )
    .bind(RAW_FACT_NORMALIZED_REPLAY_CURSOR)
    .fetch_one(&mut **tx)
    .await
    .context("failed to inspect normalized replay cursor readiness")?;

    Ok(NormalizedReplayReadiness {
        cursor_count: sql_row::get(&row, "cursor_count")?,
        remaining_block_count: sql_row::get(&row, "remaining_block_count")?,
        completed_through_block: sql_row::get(&row, "completed_through_block")?,
        caught_up: sql_row::get(&row, "caught_up")?,
    })
}

async fn load_log_staging_table_summaries(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Vec<RawFactTableSummary>> {
    let rows = sqlx::query(
        r#"
        SELECT
            c.relname AS table_name,
            stats.n_live_tup::BIGINT AS estimated_row_count,
            pg_total_relation_size(c.oid)::BIGINT AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_stat_user_tables stats
          ON stats.relid = c.oid
        WHERE n.nspname = 'public'
          AND c.relname IN ('raw_logs', 'raw_transactions', 'raw_receipts')
        ORDER BY c.relname
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("failed to inspect raw-log staging table sizes")?;

    rows.into_iter()
        .map(|row| {
            Ok(RawFactTableSummary {
                table_name: sql_row::get(&row, "table_name")?,
                estimated_row_count: sql_row::get(&row, "estimated_row_count")?,
                total_bytes: sql_row::get(&row, "total_bytes")?,
            })
        })
        .collect()
}

async fn load_uncovered_log_staging_rows(
    tx: &mut Transaction<'_, Postgres>,
    completed_through_block: i64,
) -> Result<Vec<RawFactUncoveredRows>> {
    let rows = sqlx::query(
        r#"
        SELECT table_name, row_count, max_block_number
        FROM (
            SELECT
                'raw_logs'::TEXT AS table_name,
                COUNT(*)::BIGINT AS row_count,
                MAX(block_number)::BIGINT AS max_block_number
            FROM raw_logs
            WHERE block_number > $1
            UNION ALL
            SELECT
                'raw_transactions'::TEXT AS table_name,
                COUNT(*)::BIGINT AS row_count,
                MAX(block_number)::BIGINT AS max_block_number
            FROM raw_transactions
            WHERE block_number > $1
            UNION ALL
            SELECT
                'raw_receipts'::TEXT AS table_name,
                COUNT(*)::BIGINT AS row_count,
                MAX(block_number)::BIGINT AS max_block_number
            FROM raw_receipts
            WHERE block_number > $1
        ) AS uncovered
        WHERE row_count > 0
        ORDER BY table_name
        "#,
    )
    .bind(completed_through_block)
    .fetch_all(&mut **tx)
    .await
    .context("failed to inspect raw-log staging coverage before compaction")?;

    rows.into_iter()
        .map(|row| {
            Ok(RawFactUncoveredRows {
                table_name: sql_row::get(&row, "table_name")?,
                row_count: sql_row::get(&row, "row_count")?,
                max_block_number: sql_row::get(&row, "max_block_number")?,
            })
        })
        .collect()
}

async fn truncate_log_staging_tables(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("TRUNCATE raw_logs, raw_transactions, raw_receipts")
        .execute(&mut **tx)
        .await
        .context("failed to truncate raw-log staging tables")?;

    for table in LOG_STAGING_TABLES {
        let statement = format!("ANALYZE {table}");
        sqlx::query(&statement)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("failed to analyze compacted table {table}"))?;
    }

    Ok(())
}

fn uncovered_rows_json(rows: &[RawFactUncoveredRows]) -> serde_json::Value {
    json!(
        rows.iter()
            .map(|row| {
                json!({
                    "table": row.table_name,
                    "row_count": row.row_count,
                    "max_block_number": row.max_block_number,
                })
            })
            .collect::<Vec<_>>()
    )
}

fn uncovered_rows_summary(rows: &[RawFactUncoveredRows]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "{} row_count={} max_block_number={}",
                row.table_name,
                row.row_count,
                row.max_block_number
                    .map(|block_number| block_number.to_string())
                    .unwrap_or_else(|| "NULL".to_owned())
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn raw_fact_table_summaries_json(summaries: &[RawFactTableSummary]) -> serde_json::Value {
    json!(
        summaries
            .iter()
            .map(|summary| {
                json!({
                    "table": summary.table_name,
                    "estimated_row_count": summary.estimated_row_count,
                    "total_bytes": summary.total_bytes,
                })
            })
            .collect::<Vec<_>>()
    )
}

fn total_bytes(summaries: &[RawFactTableSummary]) -> i64 {
    summaries
        .iter()
        .map(|summary| summary.total_bytes)
        .sum::<i64>()
}
