use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};

use crate::evm_primitives::normalize_evm_b256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCodeHashCorrectionCandidate {
    pub raw_code_hash_id: i64,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub contract_address: String,
    pub code_hash: String,
    pub code_byte_length: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCodeHashAddressVariant {
    pub contract_address: String,
    pub code_hashes: Vec<String>,
    pub row_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCodeHashCorrectionUpdate {
    pub raw_code_hash_id: i64,
    pub stored_code_hash: String,
    pub stored_code_byte_length: i64,
    pub corrected_code_hash: String,
    pub corrected_code_byte_length: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawCodeHashCorrectionBatchOutcome {
    pub requested_count: i64,
    pub corrected_count: i64,
    pub already_correct_count: i64,
    pub conflicting_count: i64,
}

pub async fn count_raw_code_hash_correction_candidates(
    pool: &PgPool,
    chain_id: &str,
    observed_from: OffsetDateTime,
    observed_before: OffsetDateTime,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_code_hashes AS code_hash
        WHERE code_hash.chain_id = $1
          AND code_hash.observed_at >= $2
          AND code_hash.observed_at < $3
          AND EXISTS (
              SELECT 1
              FROM chain_lineage AS lineage
              WHERE lineage.chain_id = code_hash.chain_id
                AND lineage.block_hash = code_hash.block_hash
                AND lineage.canonicality_state <> 'orphaned'::canonicality_state
          )
        "#,
    )
    .bind(chain_id)
    .bind(observed_from)
    .bind(observed_before)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count raw code-hash correction candidates for chain {chain_id}")
    })
}

pub async fn count_raw_code_hash_correction_orphaned_skips(
    pool: &PgPool,
    chain_id: &str,
    observed_from: OffsetDateTime,
    observed_before: OffsetDateTime,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_code_hashes AS code_hash
        WHERE code_hash.chain_id = $1
          AND code_hash.observed_at >= $2
          AND code_hash.observed_at < $3
          AND NOT EXISTS (
              SELECT 1
              FROM chain_lineage AS lineage
              WHERE lineage.chain_id = code_hash.chain_id
                AND lineage.block_hash = code_hash.block_hash
                AND lineage.canonicality_state <> 'orphaned'::canonicality_state
          )
        "#,
    )
    .bind(chain_id)
    .bind(observed_from)
    .bind(observed_before)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count skipped orphaned raw code-hash rows for chain {chain_id}")
    })
}

pub async fn load_raw_code_hash_correction_page(
    pool: &PgPool,
    chain_id: &str,
    observed_from: OffsetDateTime,
    observed_before: OffsetDateTime,
    after_raw_code_hash_id: i64,
    limit: i64,
) -> Result<Vec<RawCodeHashCorrectionCandidate>> {
    if limit <= 0 {
        bail!("raw code-hash correction page limit must be positive, got {limit}");
    }

    let rows = sqlx::query(
        r#"
        SELECT
            raw_code_hash_id,
            chain_id,
            LOWER(block_hash) AS block_hash,
            block_number,
            LOWER(contract_address) AS contract_address,
            LOWER(code_hash) AS code_hash,
            code_byte_length
        FROM raw_code_hashes AS code_hash
        WHERE code_hash.chain_id = $1
          AND code_hash.observed_at >= $2
          AND code_hash.observed_at < $3
          AND code_hash.raw_code_hash_id > $4
          AND EXISTS (
              SELECT 1
              FROM chain_lineage AS lineage
              WHERE lineage.chain_id = code_hash.chain_id
                AND lineage.block_hash = code_hash.block_hash
                AND lineage.canonicality_state <> 'orphaned'::canonicality_state
          )
        ORDER BY raw_code_hash_id
        LIMIT $5
        "#,
    )
    .bind(chain_id)
    .bind(observed_from)
    .bind(observed_before)
    .bind(after_raw_code_hash_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw code-hash correction page for chain {chain_id} after id {after_raw_code_hash_id}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(RawCodeHashCorrectionCandidate {
                raw_code_hash_id: row.try_get("raw_code_hash_id")?,
                chain_id: row.try_get("chain_id")?,
                block_hash: row.try_get("block_hash")?,
                block_number: row.try_get("block_number")?,
                contract_address: row.try_get("contract_address")?,
                code_hash: row.try_get("code_hash")?,
                code_byte_length: row.try_get("code_byte_length")?,
            })
        })
        .collect()
}

pub async fn load_raw_code_hash_address_variants(
    pool: &PgPool,
    chain_id: &str,
    observed_from: OffsetDateTime,
    observed_before: OffsetDateTime,
) -> Result<BTreeMap<String, RawCodeHashAddressVariant>> {
    let rows = sqlx::query(
        r#"
        SELECT
            LOWER(contract_address) AS contract_address,
            ARRAY_AGG(DISTINCT LOWER(code_hash) ORDER BY LOWER(code_hash)) AS code_hashes,
            COUNT(*)::BIGINT AS row_count
        FROM raw_code_hashes AS code_hash
        WHERE code_hash.chain_id = $1
          AND code_hash.observed_at >= $2
          AND code_hash.observed_at < $3
          AND EXISTS (
              SELECT 1
              FROM chain_lineage AS lineage
              WHERE lineage.chain_id = code_hash.chain_id
                AND lineage.block_hash = code_hash.block_hash
                AND lineage.canonicality_state <> 'orphaned'::canonicality_state
          )
        GROUP BY LOWER(contract_address)
        ORDER BY LOWER(contract_address)
        "#,
    )
    .bind(chain_id)
    .bind(observed_from)
    .bind(observed_before)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load raw code-hash address variants for chain {chain_id}")
    })?;

    rows.into_iter()
        .map(|row| {
            let contract_address = row.try_get::<String, _>("contract_address")?;
            Ok((
                contract_address.clone(),
                RawCodeHashAddressVariant {
                    contract_address,
                    code_hashes: row.try_get("code_hashes")?,
                    row_count: row.try_get("row_count")?,
                },
            ))
        })
        .collect()
}

pub async fn apply_raw_code_hash_corrections(
    pool: &PgPool,
    updates: &[RawCodeHashCorrectionUpdate],
) -> Result<RawCodeHashCorrectionBatchOutcome> {
    if updates.is_empty() {
        return Ok(RawCodeHashCorrectionBatchOutcome::default());
    }

    let updates = updates
        .iter()
        .map(normalize_correction_update)
        .collect::<Vec<_>>();
    validate_correction_updates(&updates)?;

    let raw_code_hash_ids = updates
        .iter()
        .map(|update| update.raw_code_hash_id)
        .collect::<Vec<_>>();
    let stored_code_hashes = updates
        .iter()
        .map(|update| update.stored_code_hash.clone())
        .collect::<Vec<_>>();
    let stored_code_byte_lengths = updates
        .iter()
        .map(|update| update.stored_code_byte_length)
        .collect::<Vec<_>>();
    let corrected_code_hashes = updates
        .iter()
        .map(|update| update.corrected_code_hash.clone())
        .collect::<Vec<_>>();
    let corrected_code_byte_lengths = updates
        .iter()
        .map(|update| update.corrected_code_byte_length)
        .collect::<Vec<_>>();

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open raw code-hash correction transaction")?;

    let row = sqlx::query(
        r#"
        WITH input_rows AS (
            SELECT *
            FROM unnest(
                $1::BIGINT[],
                $2::TEXT[],
                $3::BIGINT[],
                $4::TEXT[],
                $5::BIGINT[]
            ) AS input(
                raw_code_hash_id,
                stored_code_hash,
                stored_code_byte_length,
                corrected_code_hash,
                corrected_code_byte_length
            )
        ),
        current_rows AS (
            SELECT
                input_rows.raw_code_hash_id,
                target.code_hash,
                target.code_byte_length,
                input_rows.stored_code_hash,
                input_rows.stored_code_byte_length,
                input_rows.corrected_code_hash,
                input_rows.corrected_code_byte_length
            FROM input_rows
            JOIN raw_code_hashes target USING (raw_code_hash_id)
        ),
        updated AS (
            UPDATE raw_code_hashes target
            SET
                code_hash = input_rows.corrected_code_hash,
                code_byte_length = input_rows.corrected_code_byte_length
            FROM input_rows
            WHERE target.raw_code_hash_id = input_rows.raw_code_hash_id
              AND target.code_hash = input_rows.stored_code_hash
              AND target.code_byte_length = input_rows.stored_code_byte_length
              AND (
                    target.code_hash <> input_rows.corrected_code_hash
                 OR target.code_byte_length <> input_rows.corrected_code_byte_length
              )
            RETURNING target.raw_code_hash_id
        ),
        already_correct AS (
            SELECT raw_code_hash_id
            FROM current_rows
            WHERE code_hash = corrected_code_hash
              AND code_byte_length = corrected_code_byte_length
        ),
        conflicting AS (
            SELECT raw_code_hash_id
            FROM current_rows
            WHERE NOT (
                    code_hash = stored_code_hash
                AND code_byte_length = stored_code_byte_length
            )
              AND NOT (
                    code_hash = corrected_code_hash
                AND code_byte_length = corrected_code_byte_length
            )
        )
        SELECT
            (SELECT COUNT(*)::BIGINT FROM input_rows) AS requested_count,
            (SELECT COUNT(*)::BIGINT FROM current_rows) AS found_count,
            (SELECT COUNT(*)::BIGINT FROM updated) AS corrected_count,
            (SELECT COUNT(*)::BIGINT FROM already_correct) AS already_correct_count,
            (SELECT COUNT(*)::BIGINT FROM conflicting) AS conflicting_count
        "#,
    )
    .bind(&raw_code_hash_ids)
    .bind(&stored_code_hashes)
    .bind(&stored_code_byte_lengths)
    .bind(&corrected_code_hashes)
    .bind(&corrected_code_byte_lengths)
    .fetch_one(&mut *transaction)
    .await
    .context("failed to apply raw code-hash correction batch")?;

    let requested_count = row.try_get::<i64, _>("requested_count")?;
    let found_count = row.try_get::<i64, _>("found_count")?;
    let corrected_count = row.try_get::<i64, _>("corrected_count")?;
    let already_correct_count = row.try_get::<i64, _>("already_correct_count")?;
    let conflicting_count = row.try_get::<i64, _>("conflicting_count")?;
    let missing_count = requested_count - found_count;
    let orphaned_skipped_count = 0_i64;
    let accounted_count =
        corrected_count + already_correct_count + conflicting_count + orphaned_skipped_count;
    if accounted_count != requested_count {
        bail!(
            "raw code-hash correction batch accounting drift: requested {requested_count}, corrected {corrected_count}, already-correct {already_correct_count}, conflicting {conflicting_count}, orphaned-skipped {orphaned_skipped_count}"
        );
    }

    if missing_count != 0 || conflicting_count != 0 {
        bail!(
            "raw code-hash correction batch found {missing_count} missing rows and {conflicting_count} conflicting rows; refusing partial correction"
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw code-hash correction batch")?;

    Ok(RawCodeHashCorrectionBatchOutcome {
        requested_count,
        corrected_count,
        already_correct_count,
        conflicting_count,
    })
}

fn normalize_correction_update(
    update: &RawCodeHashCorrectionUpdate,
) -> RawCodeHashCorrectionUpdate {
    RawCodeHashCorrectionUpdate {
        raw_code_hash_id: update.raw_code_hash_id,
        stored_code_hash: normalize_evm_b256(&update.stored_code_hash),
        stored_code_byte_length: update.stored_code_byte_length,
        corrected_code_hash: normalize_evm_b256(&update.corrected_code_hash),
        corrected_code_byte_length: update.corrected_code_byte_length,
    }
}

fn validate_correction_updates(updates: &[RawCodeHashCorrectionUpdate]) -> Result<()> {
    for update in updates {
        if update.raw_code_hash_id <= 0 {
            bail!(
                "raw code-hash correction update id must be positive, got {}",
                update.raw_code_hash_id
            );
        }
        if update.stored_code_byte_length < 0 || update.corrected_code_byte_length < 0 {
            bail!(
                "raw code-hash correction update {} has negative byte length",
                update.raw_code_hash_id
            );
        }
        if update.stored_code_hash.is_empty() || update.corrected_code_hash.is_empty() {
            bail!(
                "raw code-hash correction update {} has empty code hash",
                update.raw_code_hash_id
            );
        }
    }
    Ok(())
}
