use anyhow::{Context, Result};
use sqlx::PgConnection;

use super::{super::super::AuthorityEventTopics, AuthorityRawLogStreamSourceRouter};

pub(in crate::ens_v1_unwrapped_authority) async fn select_authority_raw_log_stream_to_block(
    conn: &mut PgConnection,
    chain: &str,
    _source_router: &AuthorityRawLogStreamSourceRouter<'_>,
    _event_topics: &AuthorityEventTopics,
    from_block: i64,
    scan_to_block: i64,
    max_raw_logs_per_page: usize,
) -> Result<i64> {
    if from_block >= scan_to_block {
        return Ok(scan_to_block);
    }
    let max_raw_logs_per_page = i64::try_from(max_raw_logs_per_page)
        .context("authority replay max logs per page does not fit in i64")?;

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH ordered_logs AS (
            SELECT block_number
            FROM raw_logs rl
            WHERE rl.chain_id = $1
              AND rl.block_number BETWEEN $2::BIGINT AND $3::BIGINT
              AND rl.topics[1] IS NOT NULL
              AND rl.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY
                block_number,
                block_hash,
                transaction_index,
                log_index,
                raw_log_id
            LIMIT ($4::BIGINT + 1)
        ),
        numbered_logs AS (
            SELECT block_number, ROW_NUMBER() OVER () AS ordinal
            FROM ordered_logs
        ),
        overflow AS (
            SELECT block_number
            FROM numbered_logs
            WHERE ordinal = $4::BIGINT + 1
        ),
        bounded AS (
            SELECT block_number
            FROM numbered_logs
            WHERE EXISTS (SELECT 1 FROM overflow)
              AND block_number < (SELECT block_number FROM overflow)
            UNION ALL
            SELECT MIN(block_number)
            FROM numbered_logs
            WHERE EXISTS (SELECT 1 FROM overflow)
            UNION ALL
            SELECT $3::BIGINT
            WHERE NOT EXISTS (SELECT 1 FROM overflow)
        )
        SELECT COALESCE(MAX(block_number), $3::BIGINT)
        FROM bounded
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(scan_to_block)
    .bind(max_raw_logs_per_page)
    .fetch_one(&mut *conn)
    .await
    .with_context(|| {
        format!(
            "failed to select log-bounded ENSv1 unwrapped authority replay stream range for chain {chain} range {from_block}..={scan_to_block}"
        )
    })
}
