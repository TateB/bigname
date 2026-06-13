use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_raw_log,
    load::load_raw_log_internal,
    types::RawLog,
    validation::{ensure_raw_log_identity_matches, validate_raw_log},
};

/// Insert missing raw log rows or refresh canonicality for already observed
/// block-scoped logs.
pub async fn upsert_raw_logs(pool: &PgPool, logs: &[RawLog]) -> Result<Vec<RawLog>> {
    if logs.is_empty() {
        return Ok(Vec::new());
    }

    if logs.len() >= BULK_RAW_LOG_UPSERT_MIN_ROWS {
        return upsert_raw_logs_bulk(pool, logs).await;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw log upsert")?;

    let mut snapshots = Vec::with_capacity(logs.len());
    for raw_log in logs {
        validate_raw_log(raw_log)?;
        snapshots.push(upsert_raw_log(&mut transaction, raw_log).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw log upsert")?;

    Ok(snapshots)
}

/// Insert or refresh raw logs without returning row snapshots.
///
/// This keeps the same immutable-identity guard as `upsert_raw_logs`, but avoids
/// transferring and decoding row payloads for bulk backfill paths that ignore
/// the returned snapshots.
pub async fn upsert_raw_logs_without_snapshots(pool: &PgPool, logs: &[RawLog]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    for raw_log in logs {
        validate_raw_log(raw_log)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw log bulk upsert")?;

    for chunk in logs.chunks(BULK_RAW_LOG_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, log| {
            row.push_bind(&log.chain_id)
                .push_bind(&log.block_hash)
                .push_bind(log.block_number)
                .push_bind(&log.transaction_hash)
                .push_bind(log.transaction_index)
                .push_bind(log.log_index)
                .push_bind(&log.emitting_address)
                .push_bind(&log.topics)
                .push_bind(&log.data)
                .push_bind(log.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, log_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_logs.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_logs.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_logs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_logs.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_logs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_logs.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_logs.transaction_hash = EXCLUDED.transaction_hash
              AND raw_logs.block_number = EXCLUDED.block_number
              AND raw_logs.transaction_index = EXCLUDED.transaction_index
              AND raw_logs.emitting_address = EXCLUDED.emitting_address
              AND raw_logs.topics = EXCLUDED.topics
              AND raw_logs.data = EXCLUDED.data
            "#,
        );

        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to bulk upsert raw logs")?;
        if result.rows_affected() != chunk.len() as u64 {
            anyhow::bail!(
                "raw log identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw log bulk upsert")?;

    Ok(())
}

const BULK_RAW_LOG_UPSERT_MIN_ROWS: usize = 128;
const BULK_RAW_LOG_UPSERT_CHUNK_ROWS: usize = 5_000;

async fn upsert_raw_logs_bulk(pool: &PgPool, logs: &[RawLog]) -> Result<Vec<RawLog>> {
    for raw_log in logs {
        validate_raw_log(raw_log)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw log bulk upsert")?;
    let mut snapshots = Vec::with_capacity(logs.len());

    for chunk in logs.chunks(BULK_RAW_LOG_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_logs (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, log| {
            row.push_bind(&log.chain_id)
                .push_bind(&log.block_hash)
                .push_bind(log.block_number)
                .push_bind(&log.transaction_hash)
                .push_bind(log.transaction_index)
                .push_bind(log.log_index)
                .push_bind(&log.emitting_address)
                .push_bind(&log.topics)
                .push_bind(&log.data)
                .push_bind(log.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, log_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_logs.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_logs.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_logs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_logs.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_logs.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_logs.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_logs.transaction_hash = EXCLUDED.transaction_hash
              AND raw_logs.block_number = EXCLUDED.block_number
              AND raw_logs.transaction_index = EXCLUDED.transaction_index
              AND raw_logs.emitting_address = EXCLUDED.emitting_address
              AND raw_logs.topics = EXCLUDED.topics
              AND raw_logs.data = EXCLUDED.data
            RETURNING
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index,
                emitting_address,
                topics,
                data,
                canonicality_state::TEXT AS canonicality_state
            "#,
        );

        let rows = builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to bulk upsert raw logs")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "raw log identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_raw_log)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw log bulk upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_log(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    log: &RawLog,
) -> Result<RawLog> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::canonicality_state)
        ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.block_number)
    .bind(&log.transaction_hash)
    .bind(log.transaction_index)
    .bind(log.log_index)
    .bind(&log.emitting_address)
    .bind(&log.topics)
    .bind(&log.data)
    .bind(log.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })? {
        return decode_raw_log(snapshot);
    }

    let existing = load_raw_log_internal(
        &mut **executor,
        &log.chain_id,
        &log.block_hash,
        log.log_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw log for chain {} block {} log {} after insert conflict",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    ensure_raw_log_identity_matches(&existing, log)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(log.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_logs
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND log_index = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&log.chain_id)
    .bind(&log.block_hash)
    .bind(log.log_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw log for chain {} block {} log {}",
            log.chain_id, log.block_hash, log.log_index
        )
    })?;

    decode_raw_log(snapshot)
}
