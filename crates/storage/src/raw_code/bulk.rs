use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{RawCodeHash, decode_raw_code_hash, validate_raw_code_hash};

pub(super) const BULK_RAW_CODE_HASH_UPSERT_MIN_ROWS: usize = 128;
const BULK_RAW_CODE_HASH_UPSERT_CHUNK_ROWS: usize = 8_000;

pub(super) async fn upsert_raw_code_hashes_bulk(
    pool: &PgPool,
    code_hashes: &[RawCodeHash],
) -> Result<Vec<RawCodeHash>> {
    for code_hash in code_hashes {
        validate_raw_code_hash(code_hash)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw code-hash bulk upsert")?;
    let mut snapshots = Vec::with_capacity(code_hashes.len());

    for chunk in code_hashes.chunks(BULK_RAW_CODE_HASH_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO raw_code_hashes (
                chain_id,
                block_hash,
                block_number,
                contract_address,
                code_hash,
                code_byte_length,
                canonicality_state
            )
            SELECT
                chain_id,
                block_hash,
                block_number,
                contract_address,
                code_hash,
                code_byte_length,
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, code_hash| {
            row.push_bind(&code_hash.chain_id)
                .push_bind(&code_hash.block_hash)
                .push_bind(code_hash.block_number)
                .push_bind(&code_hash.contract_address)
                .push_bind(&code_hash.code_hash)
                .push_bind(code_hash.code_byte_length)
                .push_bind(code_hash.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
                chain_id,
                block_hash,
                block_number,
                contract_address,
                code_hash,
                code_byte_length,
                canonicality_state
            )
            ON CONFLICT (chain_id, block_hash, contract_address) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_code_hashes.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_code_hashes.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_code_hashes.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_code_hashes.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_code_hashes.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_code_hashes.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE raw_code_hashes.block_number = EXCLUDED.block_number
              AND raw_code_hashes.code_hash = EXCLUDED.code_hash
              AND raw_code_hashes.code_byte_length = EXCLUDED.code_byte_length
            RETURNING
                chain_id,
                block_hash,
                block_number,
                contract_address,
                code_hash,
                code_byte_length,
                canonicality_state::TEXT AS canonicality_state
            "#,
        );

        let rows = builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to bulk upsert raw code-hash rows")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "raw code-hash identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_raw_code_hash)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw code-hash bulk upsert")?;

    Ok(snapshots)
}
