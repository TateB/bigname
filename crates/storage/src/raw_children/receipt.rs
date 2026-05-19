use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_raw_receipt,
    load::load_raw_receipt_internal,
    types::RawReceipt,
    validation::{ensure_raw_receipt_identity_matches, validate_raw_receipt},
};

/// Insert missing raw receipt rows or refresh canonicality for already observed
/// block-scoped receipts.
pub async fn upsert_raw_receipts(
    pool: &PgPool,
    receipts: &[RawReceipt],
) -> Result<Vec<RawReceipt>> {
    if receipts.is_empty() {
        return Ok(Vec::new());
    }

    if receipts.len() >= BULK_RAW_RECEIPT_UPSERT_MIN_ROWS {
        return upsert_raw_receipts_bulk(pool, receipts).await;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw receipt upsert")?;

    let mut snapshots = Vec::with_capacity(receipts.len());
    for raw_receipt in receipts {
        validate_raw_receipt(raw_receipt)?;
        snapshots.push(upsert_raw_receipt(&mut transaction, raw_receipt).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw receipt upsert")?;

    Ok(snapshots)
}

/// Insert or refresh raw receipts without returning row snapshots.
pub async fn upsert_raw_receipts_without_snapshots(
    pool: &PgPool,
    receipts: &[RawReceipt],
) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }

    for raw_receipt in receipts {
        validate_raw_receipt(raw_receipt)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw receipt bulk upsert")?;

    for chunk in receipts.chunks(BULK_RAW_RECEIPT_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_receipts (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, receipt| {
            row.push_bind(&receipt.chain_id)
                .push_bind(&receipt.block_hash)
                .push_bind(receipt.block_number)
                .push_bind(&receipt.transaction_hash)
                .push_bind(receipt.transaction_index)
                .push_bind(&receipt.contract_address)
                .push_bind(receipt.status)
                .push_bind(receipt.gas_used)
                .push_bind(receipt.cumulative_gas_used)
                .push_bind(&receipt.logs_bloom)
                .push_bind(receipt.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, transaction_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_receipts.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_receipts.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_receipts.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_receipts.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_receipts.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_receipts.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_receipts.transaction_hash = EXCLUDED.transaction_hash
              AND raw_receipts.block_number = EXCLUDED.block_number
              AND raw_receipts.contract_address IS NOT DISTINCT FROM EXCLUDED.contract_address
              AND raw_receipts.status IS NOT DISTINCT FROM EXCLUDED.status
              AND raw_receipts.gas_used IS NOT DISTINCT FROM EXCLUDED.gas_used
              AND raw_receipts.cumulative_gas_used IS NOT DISTINCT FROM EXCLUDED.cumulative_gas_used
              AND raw_receipts.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
            "#,
        );

        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to bulk upsert raw receipts")?;
        if result.rows_affected() != chunk.len() as u64 {
            anyhow::bail!(
                "raw receipt identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw receipt bulk upsert")?;

    Ok(())
}

const BULK_RAW_RECEIPT_UPSERT_MIN_ROWS: usize = 128;
const BULK_RAW_RECEIPT_UPSERT_CHUNK_ROWS: usize = 5_000;

async fn upsert_raw_receipts_bulk(
    pool: &PgPool,
    receipts: &[RawReceipt],
) -> Result<Vec<RawReceipt>> {
    for raw_receipt in receipts {
        validate_raw_receipt(raw_receipt)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw receipt bulk upsert")?;
    let mut snapshots = Vec::with_capacity(receipts.len());

    for chunk in receipts.chunks(BULK_RAW_RECEIPT_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_receipts (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, receipt| {
            row.push_bind(&receipt.chain_id)
                .push_bind(&receipt.block_hash)
                .push_bind(receipt.block_number)
                .push_bind(&receipt.transaction_hash)
                .push_bind(receipt.transaction_index)
                .push_bind(&receipt.contract_address)
                .push_bind(receipt.status)
                .push_bind(receipt.gas_used)
                .push_bind(receipt.cumulative_gas_used)
                .push_bind(&receipt.logs_bloom)
                .push_bind(receipt.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, transaction_index) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_receipts.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_receipts.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_receipts.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_receipts.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_receipts.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_receipts.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_receipts.transaction_hash = EXCLUDED.transaction_hash
              AND raw_receipts.block_number = EXCLUDED.block_number
              AND raw_receipts.contract_address IS NOT DISTINCT FROM EXCLUDED.contract_address
              AND raw_receipts.status IS NOT DISTINCT FROM EXCLUDED.status
              AND raw_receipts.gas_used IS NOT DISTINCT FROM EXCLUDED.gas_used
              AND raw_receipts.cumulative_gas_used IS NOT DISTINCT FROM EXCLUDED.cumulative_gas_used
              AND raw_receipts.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
            RETURNING
                chain_id,
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                contract_address,
                status,
                gas_used,
                cumulative_gas_used,
                logs_bloom,
                canonicality_state::TEXT AS canonicality_state
            "#,
        );

        let rows = builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to bulk upsert raw receipts")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "raw receipt identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_raw_receipt)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw receipt bulk upsert")?;

    Ok(snapshots)
}

async fn upsert_raw_receipt(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    receipt: &RawReceipt,
) -> Result<RawReceipt> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        ON CONFLICT (chain_id, block_hash, transaction_index) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.block_number)
    .bind(&receipt.transaction_hash)
    .bind(receipt.transaction_index)
    .bind(&receipt.contract_address)
    .bind(receipt.status)
    .bind(receipt.gas_used)
    .bind(receipt.cumulative_gas_used)
    .bind(&receipt.logs_bloom)
    .bind(receipt.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw receipt for chain {} block {} transaction {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_hash
        )
    })? {
        return decode_raw_receipt(snapshot);
    }

    let existing = load_raw_receipt_internal(
        &mut **executor,
        &receipt.chain_id,
        &receipt.block_hash,
        receipt.transaction_index,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw receipt for chain {} block {} index {} after insert conflict",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    ensure_raw_receipt_identity_matches(&existing, receipt)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(receipt.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_receipts
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
            contract_address,
            status,
            gas_used,
            cumulative_gas_used,
            logs_bloom,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&receipt.chain_id)
    .bind(&receipt.block_hash)
    .bind(receipt.transaction_index)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw receipt for chain {} block {} index {}",
            receipt.chain_id, receipt.block_hash, receipt.transaction_index
        )
    })?;

    decode_raw_receipt(snapshot)
}
