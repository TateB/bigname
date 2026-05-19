use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_raw_transaction,
    load::load_raw_transaction_internal,
    types::RawTransaction,
    validation::{ensure_raw_transaction_identity_matches, validate_raw_transaction},
};

/// Insert missing raw transaction rows or refresh canonicality for already
/// observed block-scoped transactions.
pub async fn upsert_raw_transactions(
    pool: &PgPool,
    transactions: &[RawTransaction],
) -> Result<Vec<RawTransaction>> {
    if transactions.is_empty() {
        return Ok(Vec::new());
    }

    if transactions.len() >= BULK_RAW_TRANSACTION_UPSERT_MIN_ROWS {
        return upsert_raw_transactions_bulk(pool, transactions).await;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw transaction upsert")?;

    let mut snapshots = Vec::with_capacity(transactions.len());
    for raw_transaction in transactions {
        validate_raw_transaction(raw_transaction)?;
        snapshots.push(upsert_raw_transaction(&mut transaction, raw_transaction).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw transaction upsert")?;

    Ok(snapshots)
}

/// Insert or refresh raw transactions without returning row snapshots.
pub async fn upsert_raw_transactions_without_snapshots(
    pool: &PgPool,
    transactions: &[RawTransaction],
) -> Result<()> {
    if transactions.is_empty() {
        return Ok(());
    }

    for raw_transaction in transactions {
        validate_raw_transaction(raw_transaction)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw transaction bulk upsert")?;

    for chunk in transactions.chunks(BULK_RAW_TRANSACTION_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_transactions (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, transaction| {
            row.push_bind(&transaction.chain_id)
                .push_bind(&transaction.block_hash)
                .push_bind(transaction.block_number)
                .push_bind(&transaction.transaction_hash)
                .push_bind(transaction.transaction_index)
                .push_bind(&transaction.from_address)
                .push_bind(&transaction.to_address)
                .push_bind(transaction.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, transaction_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_transactions.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_transactions.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_transactions.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_transactions.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_transactions.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_transactions.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_transactions.transaction_hash = EXCLUDED.transaction_hash
              AND raw_transactions.block_number = EXCLUDED.block_number
              AND raw_transactions.from_address = EXCLUDED.from_address
              AND raw_transactions.to_address IS NOT DISTINCT FROM EXCLUDED.to_address
            "#,
        );

        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to bulk upsert raw transactions")?;
        if result.rows_affected() != chunk.len() as u64 {
            anyhow::bail!(
                "raw transaction identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw transaction bulk upsert")?;

    Ok(())
}

const BULK_RAW_TRANSACTION_UPSERT_MIN_ROWS: usize = 128;
const BULK_RAW_TRANSACTION_UPSERT_CHUNK_ROWS: usize = 5_000;

async fn upsert_raw_transactions_bulk(
    pool: &PgPool,
    transactions: &[RawTransaction],
) -> Result<Vec<RawTransaction>> {
    for raw_transaction in transactions {
        validate_raw_transaction(raw_transaction)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw transaction bulk upsert")?;
    let mut snapshots = Vec::with_capacity(transactions.len());

    for chunk in transactions.chunks(BULK_RAW_TRANSACTION_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_transactions (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, transaction| {
            row.push_bind(&transaction.chain_id)
                .push_bind(&transaction.block_hash)
                .push_bind(transaction.block_number)
                .push_bind(&transaction.transaction_hash)
                .push_bind(transaction.transaction_index)
                .push_bind(&transaction.from_address)
                .push_bind(&transaction.to_address)
                .push_bind(transaction.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, transaction_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_transactions.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_transactions.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_transactions.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_transactions.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_transactions.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_transactions.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_transactions.transaction_hash = EXCLUDED.transaction_hash
              AND raw_transactions.block_number = EXCLUDED.block_number
              AND raw_transactions.from_address = EXCLUDED.from_address
              AND raw_transactions.to_address IS NOT DISTINCT FROM EXCLUDED.to_address
            RETURNING
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                from_address,
                to_address,
                canonicality_state::TEXT AS canonicality_state
            "#,
        );

        let rows = builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to bulk upsert raw transactions")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "raw transaction identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_raw_transaction)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw transaction bulk upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_transaction(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    transaction: &RawTransaction,
) -> Result<RawTransaction> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_transactions (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.block_number)
    .bind(&transaction.transaction_hash)
    .bind(transaction.transaction_index)
    .bind(&transaction.from_address)
    .bind(&transaction.to_address)
    .bind(transaction.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw transaction for chain {} block {} transaction {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_hash
        )
    })? {
        return decode_raw_transaction(snapshot);
    }

    let existing = load_raw_transaction_internal(
        &mut **executor,
        &transaction.chain_id,
        &transaction.block_hash,
        transaction.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw transaction for chain {} block {} index {} after insert conflict",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    ensure_raw_transaction_identity_matches(&existing, transaction)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(transaction.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_transactions
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND transaction_index = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            from_address,
            to_address,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&transaction.chain_id)
    .bind(&transaction.block_hash)
    .bind(transaction.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw transaction for chain {} block {} index {}",
            transaction.chain_id, transaction.block_hash, transaction.transaction_index
        )
    })?;

    decode_raw_transaction(snapshot)
}
