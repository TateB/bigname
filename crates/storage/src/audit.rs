use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use crate::{CanonicalityState, ChainLineageBlock, load_chain_lineage_block};

/// Audit-facing canonicality status for one requested block identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalityInspectionStatus {
    Missing,
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}

impl From<CanonicalityState> for CanonicalityInspectionStatus {
    fn from(value: CanonicalityState) -> Self {
        match value {
            CanonicalityState::Observed => Self::Observed,
            CanonicalityState::Canonical => Self::Canonical,
            CanonicalityState::Safe => Self::Safe,
            CanonicalityState::Finalized => Self::Finalized,
            CanonicalityState::Orphaned => Self::Orphaned,
        }
    }
}

/// Block-scoped raw fact counts by storage family.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawFactAuditCounts {
    pub raw_block_count: u64,
    pub raw_code_hash_count: u64,
    pub raw_transaction_count: u64,
    pub raw_receipt_count: u64,
    pub raw_log_count: u64,
    pub raw_call_snapshot_count: u64,
}

impl RawFactAuditCounts {
    pub const fn total(&self) -> u64 {
        self.raw_block_count
            + self.raw_code_hash_count
            + self.raw_transaction_count
            + self.raw_receipt_count
            + self.raw_log_count
            + self.raw_call_snapshot_count
    }
}

/// Read-only canonicality and fact-count inspection for one block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalityInspection {
    pub chain_id: String,
    pub block_hash: String,
    pub status: CanonicalityInspectionStatus,
    pub lineage_state: Option<CanonicalityState>,
    pub parent_hash: Option<String>,
    pub block_number: Option<i64>,
    pub raw_fact_counts: RawFactAuditCounts,
    pub normalized_event_count: u64,
}

/// Stored lineage row for bounded read-only range inspection.
pub type StoredLineageRangeBlock = ChainLineageBlock;

/// Inspect one block by hash-first identity without mutating storage.
pub async fn inspect_block_canonicality(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<CanonicalityInspection> {
    validate_block_identity(chain_id, block_hash)?;

    let lineage = load_chain_lineage_block(pool, chain_id, block_hash).await?;
    let raw_fact_counts = load_raw_fact_counts(pool, chain_id, block_hash).await?;
    let normalized_event_count = load_normalized_event_count(pool, chain_id, block_hash).await?;

    Ok(build_inspection(
        chain_id,
        block_hash,
        lineage,
        raw_fact_counts,
        normalized_event_count,
    ))
}

/// Inspect every stored lineage block in a bounded block-number range. Missing
/// heights cannot be inferred without a requested block hash, so this returns
/// only stored lineage identities in range order.
pub async fn inspect_canonicality_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<CanonicalityInspection>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load lineage block hashes for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    let mut inspections = Vec::with_capacity(rows.len());
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from canonicality range row")?;
        inspections.push(inspect_block_canonicality(pool, chain_id, &block_hash).await?);
    }

    Ok(inspections)
}

/// List only stored lineage rows in a bounded block-number range. The helper
/// does not infer missing heights, gaps, range completeness, or span-wide
/// canonicality.
pub async fn list_stored_lineage_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<StoredLineageRangeBlock>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list stored lineage rows for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    rows.into_iter().map(decode_stored_lineage_block).collect()
}

fn build_inspection(
    chain_id: &str,
    block_hash: &str,
    lineage: Option<ChainLineageBlock>,
    raw_fact_counts: RawFactAuditCounts,
    normalized_event_count: u64,
) -> CanonicalityInspection {
    let status = lineage
        .as_ref()
        .map(|block| CanonicalityInspectionStatus::from(block.canonicality_state))
        .unwrap_or(CanonicalityInspectionStatus::Missing);
    let lineage_state = lineage.as_ref().map(|block| block.canonicality_state);
    let parent_hash = lineage.as_ref().and_then(|block| block.parent_hash.clone());
    let block_number = lineage.as_ref().map(|block| block.block_number);

    CanonicalityInspection {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        status,
        lineage_state,
        parent_hash,
        block_number,
        raw_fact_counts,
        normalized_event_count,
    }
}

async fn load_raw_fact_counts(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<RawFactAuditCounts> {
    let row = sqlx::query(
        r#"
        SELECT
          (SELECT COUNT(*)::BIGINT FROM raw_blocks WHERE chain_id = $1 AND block_hash = $2) AS raw_block_count,
          (SELECT COUNT(*)::BIGINT FROM raw_code_hashes WHERE chain_id = $1 AND block_hash = $2) AS raw_code_hash_count,
          (SELECT COUNT(*)::BIGINT FROM raw_transactions WHERE chain_id = $1 AND block_hash = $2) AS raw_transaction_count,
          (SELECT COUNT(*)::BIGINT FROM raw_receipts WHERE chain_id = $1 AND block_hash = $2) AS raw_receipt_count,
          (SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND block_hash = $2) AS raw_log_count,
          (SELECT COUNT(*)::BIGINT FROM raw_call_snapshots WHERE chain_id = $1 AND block_hash = $2) AS raw_call_snapshot_count
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw fact audit counts for chain {chain_id} block {block_hash}"))?;

    Ok(RawFactAuditCounts {
        raw_block_count: decode_count(&row, "raw_block_count")?,
        raw_code_hash_count: decode_count(&row, "raw_code_hash_count")?,
        raw_transaction_count: decode_count(&row, "raw_transaction_count")?,
        raw_receipt_count: decode_count(&row, "raw_receipt_count")?,
        raw_log_count: decode_count(&row, "raw_log_count")?,
        raw_call_snapshot_count: decode_count(&row, "raw_call_snapshot_count")?,
    })
}

async fn load_normalized_event_count(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<u64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS normalized_event_count
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load normalized-event audit count for chain {chain_id} block {block_hash}"
        )
    })?;

    decode_count(&row, "normalized_event_count")
}

fn decode_stored_lineage_block(row: sqlx::postgres::PgRow) -> Result<StoredLineageRangeBlock> {
    Ok(ChainLineageBlock {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        transactions_root: row
            .try_get("transactions_root")
            .context("missing transactions_root")?,
        receipts_root: row
            .try_get("receipts_root")
            .context("missing receipts_root")?,
        state_root: row.try_get("state_root").context("missing state_root")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_count(row: &sqlx::postgres::PgRow, column_name: &str) -> Result<u64> {
    let count = row
        .try_get::<i64, _>(column_name)
        .with_context(|| format!("missing {column_name}"))?;
    u64::try_from(count).with_context(|| format!("{column_name} does not fit in u64"))
}

fn validate_block_identity(chain_id: &str, block_hash: &str) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if block_hash.trim().is_empty() {
        bail!("block_hash must not be empty");
    }
    Ok(())
}

fn validate_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("canonicality inspection range start {start} is negative");
    }
    if end < start {
        bail!("canonicality inspection range end {end} is before start {start}");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
