use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ChainLineageBlock, CheckpointBlockRef,
    chain_lineage_contains_canonical_ancestor_position, load_chain_lineage_block,
    load_chain_lineage_canonical_child_path,
};
use sqlx::Row;

use crate::provider::{ProviderBlock, RAW_PAYLOAD_KIND_FULL_BLOCK};

use super::super::{
    lineage::lineage_block_to_provider,
    types::{CanonicalReconciliation, CanonicalReconciliationStatus},
};
use super::MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS;

pub(super) async fn reconcile_large_checkpoint_gap_from_stored_lineage(
    pool: &sqlx::PgPool,
    chain: &str,
    current_canonical_hash: &str,
    current_canonical_number: i64,
    latest_head: &ProviderBlock,
    selected_raw_payload_addresses: &[String],
    requires_event_silent_payloads: bool,
) -> Result<Option<CanonicalReconciliation>> {
    if latest_head.block_number <= current_canonical_number {
        return Ok(None);
    }
    let gap_blocks = latest_head.block_number - current_canonical_number;
    if gap_blocks <= MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
        return Ok(None);
    }

    let Some(stored_latest) =
        load_chain_lineage_block(pool, chain, &latest_head.block_hash).await?
    else {
        return Ok(None);
    };
    if !stored_lineage_matches_provider_block(&stored_latest, latest_head)
        || !stored_latest_is_canonical(stored_latest.canonicality_state)
    {
        return Ok(None);
    }

    let batch_blocks = usize::try_from(gap_blocks.min(MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS))
        .expect("positive live gap batch size must fit in usize");
    let path = load_chain_lineage_canonical_child_path(
        pool,
        chain,
        current_canonical_hash,
        current_canonical_number,
        batch_blocks,
    )
    .await?;
    if path.len() != batch_blocks {
        return Ok(None);
    }
    if !stored_path_has_required_raw_fact_coverage(
        pool,
        chain,
        &path,
        selected_raw_payload_addresses,
        requires_event_silent_payloads,
    )
    .await?
    {
        return Ok(None);
    }

    let target = path
        .last()
        .expect("non-empty stored lineage promotion path");
    let target_is_latest = target.block_hash == latest_head.block_hash;
    if !target_is_latest
        && !chain_lineage_contains_canonical_ancestor_position(
            pool,
            chain,
            &latest_head.block_hash,
            latest_head.block_number,
            target.block_number,
            &target.block_hash,
        )
        .await?
    {
        return Ok(None);
    }

    let canonical = CheckpointBlockRef {
        block_hash: target.block_hash.clone(),
        block_number: target.block_number,
    };
    let reconciled_blocks = path
        .into_iter()
        .rev()
        .map(|block| lineage_block_to_provider(&block))
        .collect::<Vec<_>>();

    Ok(Some(CanonicalReconciliation {
        status: CanonicalReconciliationStatus::StoredLineagePromoted,
        canonical: Some(canonical),
        fetched_parent_count: 0,
        orphaned_block_count: 0,
        reconciled_blocks,
        raw_orphan_stop_before_hash: None,
    }))
}

fn stored_latest_is_canonical(state: CanonicalityState) -> bool {
    matches!(
        state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

fn stored_lineage_matches_provider_block(
    stored: &ChainLineageBlock,
    provider: &ProviderBlock,
) -> bool {
    stored.block_hash == provider.block_hash
        && stored.parent_hash == provider.parent_hash
        && stored.block_number == provider.block_number
        && stored.block_timestamp.unix_timestamp() == provider.block_timestamp_unix_secs
        && optional_field_matches(&stored.logs_bloom, &provider.logs_bloom)
        && optional_field_matches(&stored.transactions_root, &provider.transactions_root)
        && optional_field_matches(&stored.receipts_root, &provider.receipts_root)
        && optional_field_matches(&stored.state_root, &provider.state_root)
}

fn optional_field_matches<T: Eq>(stored: &Option<T>, provider: &Option<T>) -> bool {
    matches!((stored, provider), (Some(stored), Some(provider)) if stored == provider)
        || stored.is_none()
}

async fn stored_path_has_retained_full_block_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> Result<bool> {
    if path.is_empty() {
        return Ok(true);
    }
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT block_hash)::BIGINT AS retained_block_count
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
    .bind(&block_hashes)
    .bind(RAW_PAYLOAD_KIND_FULL_BLOCK)
    .fetch_one(pool)
    .await?;
    let retained_block_count: i64 = row.try_get("retained_block_count")?;

    Ok(usize::try_from(retained_block_count)
        .map(|count| count == block_hashes.len())
        .unwrap_or(false))
}

async fn stored_path_has_required_raw_fact_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    selected_raw_payload_addresses: &[String],
    requires_event_silent_payloads: bool,
) -> Result<bool> {
    if path.is_empty() {
        return Ok(true);
    }
    if requires_event_silent_payloads {
        return stored_path_has_retained_full_block_payloads(pool, chain, path).await;
    }

    let selected_addresses = selected_raw_payload_addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if selected_addresses.is_empty() {
        return Ok(true);
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
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
                    FROM raw_payload_cache_metadata
                    WHERE raw_payload_cache_metadata.chain_id = $1
                      AND raw_payload_cache_metadata.block_hash = selected_log_blocks.block_hash
                      AND raw_payload_cache_metadata.payload_kind = $4
                      AND raw_payload_cache_metadata.retained_digest IS NOT NULL
                      AND raw_payload_cache_metadata.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_block_missing_payload_count,
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
    .bind(RAW_PAYLOAD_KIND_FULL_BLOCK)
    .fetch_one(pool)
    .await?;

    let missing_payloads: i64 = row.try_get("selected_log_block_missing_payload_count")?;
    let missing_code_hashes: i64 = row.try_get("selected_log_emitter_missing_code_count")?;
    let missing_transactions: i64 = row.try_get("selected_log_transaction_missing_count")?;
    let missing_receipts: i64 = row.try_get("selected_log_receipt_missing_count")?;

    Ok(missing_payloads == 0
        && missing_code_hashes == 0
        && missing_transactions == 0
        && missing_receipts == 0)
}
