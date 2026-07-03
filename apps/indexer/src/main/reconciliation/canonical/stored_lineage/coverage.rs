mod completed_backfill;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_manifests::{
    load_active_manifest_abi_events_by_chain_and_source_families,
    load_watched_contracts_by_addresses,
};
use bigname_storage::ChainLineageBlock;
use sqlx::Row;

use crate::provider::RAW_PAYLOAD_KIND_FULL_BLOCK;

use completed_backfill::{CompletedBackfillCoverageEvidence, completed_backfill_range_coverage};

pub(super) async fn stored_path_has_required_raw_fact_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    selected_raw_payload_addresses: &[String],
) -> std::result::Result<(), String> {
    if path.is_empty() {
        return Ok(());
    }

    let selected_addresses = selected_raw_payload_addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let selected_addresses = log_producing_selected_addresses(pool, chain, &selected_addresses)
        .await
        .map_err(|error| error.to_string())?;
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let retained_payload_hashes = retained_full_block_payload_hashes(pool, chain, &block_hashes)
        .await
        .map_err(|error| error.to_string())?;
    let completed_coverage =
        completed_backfill_range_coverage(pool, chain, path, &selected_addresses)
            .await
            .map_err(|error| error.to_string())?;
    let same_height_fork_numbers = same_height_fork_lineage_numbers(pool, chain, path)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(uncovered_block) = first_uncovered_path_block(
        path,
        &retained_payload_hashes,
        &completed_coverage,
        &same_height_fork_numbers,
        &selected_addresses,
    ) {
        return Err(format!(
            "stored lineage path over blocks {}..={} has lineage-only block {} ({}) without unambiguous completed backfill range coverage for the current watched address set and without retained full-block payload evidence; rerun hash-pinned backfill for that selected range, or use RPC-backed full-payload retention when same-height fork lineage makes numeric completed-range coverage ambiguous",
            path_start_number(path),
            path_end_number(path),
            uncovered_block.block_number,
            uncovered_block.block_hash
        ));
    }

    if selected_addresses.is_empty() {
        return Ok(());
    }

    let row = sqlx::query(
        r#"
        WITH selected_log_blocks AS (
            SELECT DISTINCT raw_logs.block_hash
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND LOWER(raw_logs.emitting_address) = ANY($3::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        selected_log_emitters AS (
            SELECT DISTINCT
                raw_logs.block_hash,
                LOWER(raw_logs.emitting_address) AS emitting_address
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND LOWER(raw_logs.emitting_address) = ANY($3::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        selected_log_transactions AS (
            SELECT DISTINCT
                raw_logs.block_hash,
                raw_logs.transaction_hash,
                raw_logs.transaction_index
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND LOWER(raw_logs.emitting_address) = ANY($3::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        SELECT
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_blocks
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM UNNEST($4::TEXT[]) AS covered(block_hash)
                    WHERE covered.block_hash = selected_log_blocks.block_hash
                )
            ) AS selected_log_block_missing_coverage_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_emitters
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_code_hashes
                    WHERE raw_code_hashes.chain_id = $1
                      AND raw_code_hashes.block_hash = selected_log_emitters.block_hash
                      AND LOWER(raw_code_hashes.contract_address) = selected_log_emitters.emitting_address
                      AND raw_code_hashes.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_emitter_missing_code_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_transactions
                    WHERE raw_transactions.chain_id = $1
                      AND raw_transactions.block_hash = selected_log_transactions.block_hash
                      AND raw_transactions.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_transactions.transaction_index = selected_log_transactions.transaction_index
                      AND raw_transactions.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_transaction_missing_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_receipts
                    WHERE raw_receipts.chain_id = $1
                      AND raw_receipts.block_hash = selected_log_transactions.block_hash
                      AND raw_receipts.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_receipts.transaction_index = selected_log_transactions.transaction_index
                      AND raw_receipts.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_receipt_missing_count
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .bind(&selected_addresses)
    .bind(covered_block_hashes(
        path,
        &retained_payload_hashes,
        &completed_coverage,
        &same_height_fork_numbers,
        &selected_addresses,
    ))
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;

    let missing_coverage: i64 = row
        .try_get("selected_log_block_missing_coverage_count")
        .map_err(|error| error.to_string())?;
    let missing_code_hashes: i64 = row
        .try_get("selected_log_emitter_missing_code_count")
        .map_err(|error| error.to_string())?;
    let missing_transactions: i64 = row
        .try_get("selected_log_transaction_missing_count")
        .map_err(|error| error.to_string())?;
    let missing_receipts: i64 = row
        .try_get("selected_log_receipt_missing_count")
        .map_err(|error| error.to_string())?;

    if missing_coverage != 0 {
        return Err(format!(
            "stored lineage selected-log blocks over {}..={} lack completed backfill range coverage or retained full-block payload evidence; rerun hash-pinned backfill for the current watched address set before retrying",
            path_start_number(path),
            path_end_number(path)
        ));
    }
    if missing_code_hashes != 0 || missing_transactions != 0 || missing_receipts != 0 {
        return Err(format!(
            "stored lineage selected logs over {}..={} are missing raw code/transaction/receipt companion rows (missing code: {missing_code_hashes}, transactions: {missing_transactions}, receipts: {missing_receipts}); rerun hash-pinned backfill for the selected range before retrying",
            path_start_number(path),
            path_end_number(path)
        ));
    }

    Ok(())
}

async fn log_producing_selected_addresses(
    pool: &sqlx::PgPool,
    chain: &str,
    selected_addresses: &[String],
) -> Result<Vec<String>> {
    if selected_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let targets = selected_addresses
        .iter()
        .map(|address| (chain.to_owned(), address.clone()))
        .collect::<Vec<_>>();
    let watched_contracts = load_watched_contracts_by_addresses(pool, &targets).await?;
    if watched_contracts.is_empty() {
        return Ok(selected_addresses.to_vec());
    }

    let source_families = watched_contracts
        .iter()
        .map(|contract| contract.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source_families_with_topics =
        load_active_manifest_abi_events_by_chain_and_source_families(pool, chain, &source_families)
            .await?
            .into_iter()
            .filter(|event| event.topic0.is_some())
            .map(|event| event.source_family)
            .collect::<BTreeSet<_>>();

    let mut source_families_by_address = BTreeMap::<String, BTreeSet<String>>::new();
    for contract in watched_contracts {
        source_families_by_address
            .entry(contract.address.to_ascii_lowercase())
            .or_default()
            .insert(contract.source_family);
    }

    Ok(selected_addresses
        .iter()
        .filter(|address| {
            source_families_by_address
                .get(*address)
                .is_none_or(|families| {
                    families
                        .iter()
                        .any(|family| source_families_with_topics.contains(family))
                })
        })
        .cloned()
        .collect())
}

async fn retained_full_block_payload_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<Vec<String>> {
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT block_hash
        FROM raw_payload_cache_metadata
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND payload_kind = $3
          AND retained_digest IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(RAW_PAYLOAD_KIND_FULL_BLOCK)
    .fetch_all(pool)
    .await?;

    let mut hashes = Vec::with_capacity(rows.len());
    for row in rows {
        hashes.push(row.try_get("block_hash")?);
    }
    Ok(hashes)
}

async fn same_height_fork_lineage_numbers(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> Result<BTreeSet<i64>> {
    if path.is_empty() {
        return Ok(BTreeSet::new());
    }

    let block_numbers = path
        .iter()
        .map(|block| block.block_number)
        .collect::<Vec<_>>();
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT block_number
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = ANY($2::BIGINT[])
          AND NOT (block_hash = ANY($3::TEXT[]))
        "#,
    )
    .bind(chain)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .fetch_all(pool)
    .await?;

    let mut numbers = BTreeSet::new();
    for row in rows {
        numbers.insert(row.try_get("block_number")?);
    }
    Ok(numbers)
}

fn first_uncovered_path_block<'a>(
    path: &'a [ChainLineageBlock],
    retained_payload_hashes: &[String],
    completed_coverage: &CompletedBackfillCoverageEvidence,
    same_height_fork_numbers: &BTreeSet<i64>,
    selected_addresses: &[String],
) -> Option<&'a ChainLineageBlock> {
    path.iter().find(|block| {
        !retained_payload_hashes
            .iter()
            .any(|hash| hash.eq_ignore_ascii_case(&block.block_hash))
            && (same_height_fork_numbers.contains(&block.block_number)
                || !completed_coverage.covers_block(block.block_number, selected_addresses))
    })
}

fn covered_block_hashes(
    path: &[ChainLineageBlock],
    retained_payload_hashes: &[String],
    completed_coverage: &CompletedBackfillCoverageEvidence,
    same_height_fork_numbers: &BTreeSet<i64>,
    selected_addresses: &[String],
) -> Vec<String> {
    let mut hashes = retained_payload_hashes.to_vec();
    hashes.extend(
        path.iter()
            .filter(|block| {
                !same_height_fork_numbers.contains(&block.block_number)
                    && completed_coverage.covers_block(block.block_number, selected_addresses)
            })
            .map(|block| block.block_hash.clone()),
    );
    hashes.sort();
    hashes.dedup();
    hashes
}

fn path_start_number(path: &[ChainLineageBlock]) -> i64 {
    path.first()
        .expect("stored lineage path must not be empty")
        .block_number
}

fn path_end_number(path: &[ChainLineageBlock]) -> i64 {
    path.last()
        .expect("stored lineage path must not be empty")
        .block_number
}
